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
- **Kubernetes cluster with agent-sandbox**: `agent-sandbox` is a prerequisite for running `eval-runner`. When deploying with `./dev/tasks/deploy-to-kube` (or `./dev/tasks/deploy-to-gke`), `agent-sandbox` is installed automatically. To install the pinned `v1.0.7` release manually on any cluster where `eval-runner` is deployed:

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

Because container image builds use the repository root as their build context, a sibling module referenced via `go.work` (e.g. `../agent-sandbox`) is not included in the image build context. Multi-module `go.work` setups pointing to sibling directories are therefore local-only workflows (or the sibling module must be vendored into the tree). For container image builds, the pinned `replace` directive in `go.mod` is used instead.

#### CI Builds via `replace`
In `go.mod`, a pinned `replace` directive resolves `sigs.k8s.io/agent-sandbox` to the fork commit on GitHub so CI builds succeed without requiring a multi-module workspace:
```
replace sigs.k8s.io/agent-sandbox => github.com/codebot-robot/agent-sandbox <commit>
```

#### Building the `sandboxd` Image
To build the `sandboxd` image from the fork:
```bash
./dev/tasks/sandboxd-image
```
Pass `--push` to push the image to a container registry, or pass `IMAGE_PREFIX` and `IMAGE_TAG` to customize the tag.
Pass `--clone-only` to prepare the checkout without building the image.

`dev/tasks/sandboxd-image` uses a sibling checkout (`../agent-sandbox`) if present, or clones the pinned commit outside the repository into `${XDG_CACHE_HOME:-~/.cache}/eval-runner/agent-sandbox` (configurable via `AGENT_SANDBOX_DIR`). The fork is intentionally never cloned under the repository tree.

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

### Task Script Discovery in `dev/tasks/`

`ap` automatically discovers executable scripts located in `dev/tasks/` based on reserved prefixes:
- `build-*`: Executed during `ap build` (and therefore `ap deploy`).
- `test-*`: Executed during `ap test`.
- `lint-*`: Executed during `ap lint`.
- `test-e2e*`: Executed during `ap e2e`.

New scripts added to `dev/tasks/` must pick prefixes deliberately. Standalone utility or maintenance scripts (such as `sandboxd-image` or `deploy-to-kube`) must use names outside these reserved prefixes so they are not run unexpectedly during every build or deploy cycle.

## Generating CRDs and DeepCopy

CRD manifests under `k8s/` and Go `runtime.Object` deepcopy methods are generated using `controller-gen`.

Run:

```bash
./dev/tasks/generate-crds
```

This task is also invoked automatically whenever `ap generate` is executed.

## Building Container Images (`ap build`)

`eval-runner` builds three primary Go container images:
- `images/eval-runner/Dockerfile` (the controller)
- `images/eval-runner-ui/Dockerfile` (the web UI)
- `images/eval-runner-webhook/Dockerfile` (the GitHub webhook server)

### Full Git Source Tree Builds and VCS Stamping

Each Go image builds from the full git tree using `COPY . .` rather than a hand-maintained list of subdirectories. Builds use Go package paths (`go build -o <bin> ./cmd/<name>`) rather than file arguments:
- **Automatic VCS Stamping**: Go embeds version control metadata (`vcs.revision`, `vcs.time`, and `vcs.modified`) into the binary via `runtime/debug.ReadBuildInfo()` without needing `-ldflags` or manual build arguments.
- **Reporting and Visibility**:
  - The controller logs the stamped VCS revision at startup (`{"version": "<sha>[-dirty]"}`).
  - The UI displays `eval-runner <sha>[-dirty]` in page footers.
  - All three binaries support `--version` (or `-version`) to print the stamped git revision and exit 0 before any Kubernetes client or network connection is initialized (allowing `docker run --rm <image> --version` to work in distroless environments without cluster access).
- **Caching**: Docker dependency caching is preserved by running `COPY go.mod go.sum ./` and `RUN go mod download` with cache mounts (`--mount=type=cache,target=/go/pkg/mod` and `/root/.cache/go-build`) before `COPY . .`.
- **`.dockerignore`**: A top-level `.dockerignore` excludes ephemeral build outputs (`.build/`, `bin/`), local workspace configurations (`go.work`, `go.work.sum`), credentials, editor configs, and temporary caches, while explicitly keeping `.git` in the context so VCS stamping works. Note: because Go's VCS stamping runs `git status --porcelain` inside the build context, `.dockerignore` must not exclude any git-tracked files, or git inside the container will perceive them as deleted and stamp `-dirty`.

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

### Deploying to any Kubernetes cluster in one step (`dev/tasks/deploy-to-kube`)

For generic Kubernetes clusters (kind, kops, EKS, etc.), `./dev/tasks/deploy-to-kube` (bash, standard library tools only) handles cluster bring-up and deployment idempotently in one step:
1. Ensures namespaces `eval-runner` and `eval-runner-sandbox` exist with restricted pod-security labels (`k8s/00-namespaces.yaml`).
2. Installs `agent-sandbox` at the pinned release (`v1.0.7`, shared with the e2e test harness via `dev/tasks/versions.sh`), waits for the CRD to be established, and waits for the controller deployment rollout.
3. Builds and makes the pinned fork `sandboxd` image available under `IMAGE_PREFIX` (or reuses an existing image with `--skip-sandboxd-build`). When built on an arm64 host, it produces a `linux/amd64` image by default so x86 cluster nodes can execute it.
4. Writes the `eval-runner-config` ConfigMap with `sandboxdImage` and any extra settings supplied via `--set KEY=VALUE`.
5. Verifies whether Secret `eval-runner-sandbox/model-credentials` exists and prints instructions on creating it if absent.
6. Invokes `./dev/tasks/deploy`, waits for controller, UI, and webhook rollouts, and confirms effective cluster settings took effect.

Usage:

```bash
IMAGE_PREFIX=<registry> ./dev/tasks/deploy-to-kube
```

On kind clusters (active context matching `kind-*`), `IMAGE_PREFIX` defaults to `images.local`, `IMAGE_TAG` defaults to `dev`, and `sandboxd` is loaded directly into the kind node. On real clusters, `IMAGE_PREFIX` is required (e.g. `IMAGE_PREFIX=gcr.io/my-project`).

Options:
- `--set KEY=VALUE`: Merges extra ConfigMap entries (repeatable), for example `--set artifactBucket=my-bucket` or `--set defaultModel=gemini-2.5-flash`.
- `--skip-sandboxd-build`: Skips building and pushing `sandboxd`, reusing an already pushed image index.
- `--skip-deploy`: Provisions namespaces, `agent-sandbox`, `sandboxd`, and ConfigMap without running `./dev/tasks/deploy`.
- `--restart`: Restarts deployments after deploy.
- `--dry-run`: Prints commands without executing mutating actions.

If `artifactBucket` is configured, the controller and UI interact with GCS using Application Default Credentials (ADC). On non-GKE clusters, ensure the node service account (or cloud IAM role provider) is granted `roles/storage.objectAdmin` on the bucket.

### Deploying to GKE in one step (`dev/tasks/deploy-to-gke`)

For a fresh GKE cluster, `./dev/tasks/deploy-to-gke` (Python, standard library only) performs the GKE-specific cloud setup and delegates generic cluster bring-up and deployment to `./dev/tasks/deploy-to-kube`:

```bash
./dev/tasks/deploy-to-gke --cluster projects/<project>/locations/<region>/clusters/<cluster>
```

It:
1. Fetches cluster credentials into a dedicated kubeconfig (`~/.kube/eval-runner-<project>-<cluster>.kubeconfig`, never `~/.kube/config`, so your default context is untouched).
2. Creates the artifact bucket `<project>-eval-runner` if needed.
3. Grants the controller and UI Kubernetes ServiceAccounts access to the bucket through Workload Identity Federation principals (`principal://iam.googleapis.com/...`) without needing Google Service Account keys or KSA annotations.
4. Delegates the remainder to `./dev/tasks/deploy-to-kube` with `IMAGE_PREFIX=gcr.io/<project>` and `--set artifactBucket=... --set artifactDir=none`.

Every step is idempotent; `--dry-run` prints the commands instead of running them. Use `--set KEY=VALUE` for extra ConfigMap entries, `--bucket`, `--image-prefix` and `--kubeconfig` to override defaults, and `--skip-bucket`, `--skip-sandboxd-build`, or `--skip-deploy` to run only part of it. Use `--restart` to restart deployments after deploying if running a controller older than #277 (the controller and UI reload at runtime since #277).

Granting bucket access needs `storage.buckets.setIamPolicy`. If that step is denied, the script prints the account you are authenticated as and suggests the project-level grant to ask for (or to give yourself, if you administer the project):

```bash
gcloud projects add-iam-policy-binding <project> --member="user:<you>" --role="roles/storage.admin"
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
- `http://localhost:8080/`: repository index listing distinct repositories and counts in the namespace.
- `http://localhost:8080/<host>/<owner>/<repo>`: repository home page and Overview tab displaying overall eval score headline with 95% interval, eval suites and evals with main-branch scores, deltas, and observation trend sparklines.
- `http://localhost:8080/<host>/<owner>/<repo>/badge.svg`: embeddable Shields-style flat badge with pooled score and 95% CI.
- `http://localhost:8080/<host>/<owner>/<repo>/sparkline.svg`: embeddable SVG sparkline trend over the last ≤ 20 commits with shaded CI band.
- `http://localhost:8080/<host>/<owner>/<repo>/badge.json`: shields.io endpoint JSON schema for custom badge styling.
- `http://localhost:8080/<host>/<owner>/<repo>/runs`: view all Runs for the repository.
- `http://localhost:8080/<host>/<owner>/<repo>/comparisons`: view all Comparisons for the repository.
- `http://localhost:8080/<host>/<owner>/<repo>/pulls`: view all pull requests for the repository with latest verdicts, base → head commit SHAs, samples, batches, completion time, and age.
- `http://localhost:8080/<host>/<owner>/<repo>/pulls/<number>`: view verdict history across pushes for a pull request with links to the Comparison and both candidate and baseline Runs.
- `http://localhost:8080/pulls`: view all pull requests derived from Comparisons, grouped by repository and PR number with latest verdicts, base → head commit SHAs, samples, batches, completion time, and age.
- `http://localhost:8080/pulls/<repohash>/<number>`: view verdict history across pushes for a pull request with links to the Comparison and both candidate and baseline Runs.
- `http://localhost:8080/runs`: view all Runs in the namespace with their phase, task completion, sample counts, and rates.
- `http://localhost:8080/runs/<namespace>/<name>`: view detailed Run statistics with per-task rates, 95% CI bars against a 0–1 axis, sample counts, estimation method, and artifact pointers.
- `http://localhost:8080/runs/<namespace>/<name>/tasks/<task>/samples`: per-task sample list with scores, preview snippets, latency, and sample counts.
- `http://localhost:8080/runs/<namespace>/<name>/tasks/<task>/samples/<sample>`: sample transcript viewer rendering multi-turn message and event streams as typed text rows.
- `http://localhost:8080/comparisons/<namespace>/<name>`: view paired comparison results in the PR-comment table shape with Δ CI bars drawn against a zero line, verdict chip, P(improvement), and base/candidate commits. The top of the page shows the Quality, Tokens and Cost changes between the two Runs, `status.message` in a Reason panel, and a Regressed tasks table (base → candidate rate, Δ, and 95% CI) when any task is labeled `regressed`. A message with line breaks renders as a headline (its first line) followed by blocks: blank lines separate blocks, consecutive `- ` lines form a list, a line ending in `:` directly above a list is its heading, other lines keep their line breaks, and `http(s)` URLs become links. A message without line breaks renders as one block.
- `http://localhost:8080/comparisons`: view all Comparisons in the namespace. Each row shows the completion time in UTC, a reason line under the verdict (the first line of `status.message`, or for a `REGRESSED` Comparison without a message, the regressed tasks with the most negative Δ), and the Quality, Tokens and Cost changes between its two Runs. Comparisons labeled `evalrunner.gke-labs.dev/vs-baseline: "true"` or `evalrunner.gke-labs.dev/vs-previous: "true"` are grouped into one row per candidate Run with side-by-side "vs baseline" and "vs previous" cells, ordered by candidate Run creation time; `?kind=baseline` or `?kind=previous` shows one kind only.
- `http://localhost:8080/series`: view all main-branch series across repositories.
- `http://localhost:8080/series/<repohash>/<branch>`: view main-branch progression series chart with confidence interval bands and per-task rates over time.

#### Task Routes, Path Decoding, and Escape Hatches

The UI serves task sample lists and individual sample transcripts under `http://localhost:8080/runs/<namespace>/<name>/tasks/<task>/samples[/<sample>]`. Internally, these routes are registered as `GET /runs/{namespace}/{name}/tasks` and `GET /runs/{namespace}/{name}/tasks/{rest...}`.

**Why `{rest...}` is used**:
Task IDs frequently contain slashes (such as `docs/readme_comprehension`). The UI generates canonical links using URL-encoded task IDs (`url.PathEscape`, e.g. `docs%2Freadme_comprehension`). However, ingress controllers, reverse proxies, and cloud load balancers often decode `%2F` into `/` before proxying requests to the container. If the route were registered with single-segment `{task}` (such as `.../tasks/{task}/samples`), Go's router would treat each slash as a path separator, causing decoded multi-segment paths to return 404.

The `{rest...}` wildcard captures the full trailing path. The server extracts the task ID and optional sample ID by splitting backwards at the last `/samples` delimiter:
- Canonical `%2F`-encoded paths (`.../tasks/docs%2Freadme_comprehension/samples`) and proxy-decoded paths (`.../tasks/docs/readme_comprehension/samples`) resolve to the same task sample list.
- Sample transcripts (`.../samples/<sample>`) preserve `#`-suffixed sample IDs (such as epoch identifiers `sample-017#2`).

**Escape-Hatch Query Form**:
If intermediate proxies or gateways rewrite paths in unsupported ways, query parameters provide an explicit escape hatch that bypasses path parsing entirely:
- `http://localhost:8080/runs/<namespace>/<name>/tasks?task=<task>`: opens the sample list for `<task>`.
- `http://localhost:8080/runs/<namespace>/<name>/tasks?task=<task>&sample=<sample>`: opens the transcript for `<sample>`.

Query parameters take precedence over path-extracted values when provided.

**Repository-Scoped Routes**:
Once #241 merges, equivalent routes will be registered under `http://localhost:8080/<host>/<owner>/<repo>/runs/<name>/tasks/<task>/samples[/<sample>]` (as `/{host}/{owner}/{repo}/runs/{name}/tasks/{rest...}`), supporting the same decoded multi-segment paths and `?task=&sample=` query escape hatch.

#### Diagnosing Missing Artifact Storage

The UI requires access to evaluation logs via an `ArtifactSink` (`artifactBucket` or `artifactDir` in `eval-runner-config`). When artifact storage is missing or not configured:
- **Page Footer**: Run detail, sample list, and transcript pages display an `artifacts: ...` footer line (`artifacts: gs://<bucket>`, `artifacts: dir <path>`, or `artifacts: none`) alongside the application version (`eval-runner <version>`). This allows operators to quickly verify whether the UI is connected to an artifact sink and check the running version without running `kubectl`.
- **Nil-Sink Guidance Message**: Navigating to a task sample list or sample transcript when no sink is configured renders an actionable guidance message rather than a 404:
  > `artifact storage is not configured for the UI; set artifactBucket or artifactDir in eval-runner-config`

  The missing sink is also logged in the UI container output (`artifact storage not configured for UI`).
- **Run Detail Page**: On `http://localhost:8080/runs/<namespace>/<name>`, task IDs that have a log path are displayed as plain text without links alongside an `(artifacts unavailable)` note when artifact storage is unconfigured, preventing navigation to unbacked pages.

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

### NetworkPolicy Enforcement and Debugging

Network isolation for sandboxes is defined via `eval-runner-sandbox-ingress` (static manifest in `k8s/sandbox-networkpolicy.yaml`) and `eval-runner-sandbox-egress` (dynamically owned and reconciled by the controller), applying to all pods labeled `evalrunner.gke-labs.dev/sandbox: "true"`.

#### FQDN Narrowing on GKE Dataplane V2 (`FQDNNetworkPolicy`)

Standard Kubernetes `NetworkPolicy` operates at L3/L4 (IP blocks and CIDRs) and cannot express domain names, meaning baseline egress policies without domain controls must allow port 443 to `0.0.0.0/0` (external IPs) so sandboxes can reach git hosts and model APIs.

On GKE Dataplane V2 clusters, the controller dynamically renders and reconciles an `FQDNNetworkPolicy` (`networking.gke.io/v1alpha1`, ADR-029) named `eval-runner-sandbox-fqdn-egress` in `eval-runner-sandbox` that selects `evalrunner.gke-labs.dev/sandbox: "true"` and specifies the allowed hostnames for HTTPS egress on port 443.

**Allow-Union Semantics and Controller Ownership**: On GKE Dataplane V2, an `FQDNNetworkPolicy` combines with standard `NetworkPolicy` resources selecting the same Pod as an **allow-union** (traffic is allowed if either policy matches). To achieve true egress narrowing, the controller owns both the baseline `eval-runner-sandbox-egress` NetworkPolicy and the `eval-runner-sandbox-fqdn-egress` FQDN policy. Whenever FQDN narrowing is active, the controller drops the `0.0.0.0/0` internet rule (allowing TCP 80/443/9418) from `eval-runner-sandbox-egress`, leaving only DNS (UDP/TCP 53) and the internal git fixture (TCP 9418). All HTTPS traffic on port 443 is governed exclusively by the `FQDNNetworkPolicy`. Consequently, probes to unlisted domains (e.g. `curl -sSf https://example.com`) from a sandbox will time out on clusters with `--enable-fqdn-network-policy`. When FQDN narrowing is absent or disabled (`sandboxEgressHosts: "none"`), the controller restores the 3-rule baseline including `0.0.0.0/0:80/443/9418`.

- **GKE-only feature**: FQDN policies require GKE Dataplane V2 with FQDN network policy enabled. Clusters without Dataplane V2 (e.g. local `kind` clusters running `kindnet`) retain the controller-rendered 3-rule baseline `NetworkPolicy` without breaking or degrading.
- **Enabling FQDN Network Policy on GKE**: The cluster must be running GKE Dataplane V2 (GKE >= 1.26.4-gke.500 / 1.27.1-gke.400) with FQDN network policy enabled. The CRD is installed by this feature flag, not by Dataplane V2 alone:
  ```bash
  gcloud container clusters update CLUSTER --enable-fqdn-network-policy
  ```
  Verify that the CRD is available in the cluster:
  ```bash
  kubectl api-resources --api-group=networking.gke.io
  ```
  The controller checks for this CRD at startup and logs whether FQDN policy management is enabled or disabled until restart.
- **ConfigMap Key (`sandboxEgressHosts`)**: The allow-list of hostnames is driven by the ConfigMap key `sandboxEgressHosts` in `eval-runner-config` (or CLI flag `--sandbox-egress-hosts`).
- **Built-in Defaults**: If `sandboxEgressHosts` is omitted, the controller defaults to:
  - Model APIs: `*.googleapis.com` (covers `generativelanguage.googleapis.com`, etc.)
  - Git hosts: `github.com`, `*.github.com`, `*.githubusercontent.com`
  - Package repositories: `pypi.org`, `*.pypi.org`, `files.pythonhosted.org`, `*.pythonhosted.org`
- **GKE FQDN Constraints**:
  - `pattern: *.googleapis.com` matches exactly one DNS label (e.g. `generativelanguage.googleapis.com` matches, but multi-label subdomains like `a.b.googleapis.com` do not).
  - A single `FQDNNetworkPolicy` resolves at most 50 IP addresses; the default set of `*.googleapis.com` plus GitHub and PyPI endpoints may approach that under load.
- **Extending the List**: Operators can extend the allow-list without code changes by setting `sandboxEgressHosts` in `eval-runner-config`:
  ```yaml
  apiVersion: v1
  kind: ConfigMap
  metadata:
    name: eval-runner-config
    namespace: eval-runner
  data:
    sandboxEgressHosts: "*.googleapis.com, github.com, *.github.com, *.githubusercontent.com, pypi.org, *.pypi.org, files.pythonhosted.org, *.pythonhosted.org, api.openai.com, *.anthropic.com"
  ```
  Wildcard domains (containing `*`) are rendered as `pattern` matchers, while exact domains are rendered as `name` matchers.
- **Disabling FQDN Egress**: Setting `sandboxEgressHosts: "none"` disables the FQDN policy and removes the `FQDNNetworkPolicy` object.
- **Hot-Reloading**: Changes to `sandboxEgressHosts` in `eval-runner-config` are reloaded at runtime by `ConfigReloader` without restarting the controller pod.

#### Enforcement Prerequisites

Enforcement requires a cluster CNI with NetworkPolicy support enabled, such as GKE Dataplane V2 (e.g. `gke-labs-corp-dev`) or Calico.
- **Local `kind` clusters**: kind's default `kindnet` CNI does not enforce NetworkPolicy; manifests apply cleanly and rules are verified by unit tests in `pkg/discovery/networkpolicy_test.go`.
- **GKE Dev Cluster (`ai-us-central1-b`)**: Uses the legacy datapath with the network policy addon disabled and does not enforce NetworkPolicies.

#### Inspecting Applied Policies

To inspect the applied policies in the sandbox namespace:

```bash
kubectl -n eval-runner-sandbox describe networkpolicy eval-runner-sandbox-ingress
kubectl -n eval-runner-sandbox describe networkpolicy eval-runner-sandbox-egress
# On GKE Dataplane V2 clusters with FQDN policy enabled:
kubectl -n eval-runner-sandbox describe fqdnnetworkpolicy eval-runner-sandbox-fqdn-egress
```

#### Dataplane V2 Deny Logging

On GKE clusters with Dataplane V2, you can enable policy action logging to observe allowed and denied connections:

```bash
kubectl apply -f - <<EOF
apiVersion: networking.gke.io/v1alpha1
kind: NetworkLogging
metadata:
  name: default
spec:
  cluster:
    allow:
      delegate: true
      log: false
    deny:
      delegate: true
      log: true
EOF
```

##### Debugging a Blocked Host

When an Inspect task or git clone fails with connection timeouts or `Connection refused`:
1. Check Cloud Logging for dropped egress traffic:
   ```
   resource.type="k8s_node"
   jsonPayload.disposition="DENY"
   jsonPayload.dest_port=443
   ```
2. Or inspect the node's local log at `/var/log/network/policy_action.log`.
3. If an evaluation task requires an external API or package index not in the default allow-list, add the required hostname(s) to `sandboxEgressHosts` in `eval-runner-config`.

#### Manual Verification Probes (GKE Dataplane V2)

To manually verify ingress and egress policy enforcement on an enforcing cluster:

1. **Verify Ingress Enforcement**:
   - Run a labelled listener in `eval-runner-sandbox` (use images like `busybox:1.36` or `alpine` with `nc` pre-installed, since egress package managers are blocked):
     ```bash
     kubectl run -n eval-runner-sandbox listener --image=busybox:1.36 --labels="evalrunner.gke-labs.dev/sandbox=true" -- nc -l -p 8080
     ```
   - From another namespace (e.g. `default`), attempt to connect:
     ```bash
     kubectl run -n default probe --rm -i --tty --image=busybox:1.36 -- nc -zvw3 listener.eval-runner-sandbox.svc 8080
     ```
   - **Expected**: Connection fails / times out (blocked by ingress policy; only controller pods in `eval-runner` can connect).

2. **Verify Egress Enforcement**:
   - Run a probe pod in `eval-runner-sandbox` with the sandbox label and no ServiceAccount token:
     ```bash
     kubectl -n eval-runner-sandbox run np-probe --rm -it --restart=Never --image=busybox:1.36 --labels=evalrunner.gke-labs.dev/sandbox=true --overrides='{"spec":{"automountServiceAccountToken":false}}' -- sh
     ```
   - From within the probe pod:
     - `nslookup github.com`: resolves (DNS egress to kube-system CoreDNS / link-local DNSCache succeeds).
     - `nc -zvw3 git-server.eval-runner-sandbox.svc.cluster.local 9418`: succeeds (in-cluster git fixture on port 9418).
     - `wget -qO- --timeout=5 https://api.github.com/zen`: succeeds (external HTTPS 443 to allowed host).
     - `nc -zvw3 generativelanguage.googleapis.com 443`: succeeds (model API HTTPS 443 to allowed host).
     - `nc -zvw3 eval-runner-controller.eval-runner.svc.cluster.local 8081`: times out (controller probe endpoint blocked).
     - `nc -zvw3 eval-runner-ui.eval-runner.svc.cluster.local 80`: times out (controller namespace blocked).
     - `wget -qO- --timeout=3 http://169.254.169.254/computeMetadata/v1/ --header 'Metadata-Flavor: Google'`: times out (GCE metadata server on :80 blocked).
     - `nc -zvw3 example.com 8080`: times out (default-deny egress on non-whitelisted ports).
     - `nc -zvw3 kubernetes.default.svc.cluster.local 443`: times out if control plane is private (`10.x`); if control plane is a public IP, connection succeeds at L4 but requests without credentials return 401/403 (`automountServiceAccountToken: false`).

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

`./dev/tasks/deploy-to-gke` performs this setup automatically using Workload Identity Federation principals (`principal://iam.googleapis.com/projects/<number>/locations/global/workloadIdentityPools/<project>.svc.id.goog/subject/ns/eval-runner/sa/eval-runner-controller`) bound directly on the bucket. The steps below are the manual alternative using a Google Service Account:

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

### UI Artifact Storage Access (Workload Identity)

To allow the web dashboard (`eval-runner-ui`) to read Inspect logs and display sample lists and trajectory transcripts directly from GCS using Application Default Credentials (ADC, no Secret required):

1. **Create the Google Service Account (GSA) and grant `roles/storage.objectViewer`:**

   ```bash
   gcloud iam service-accounts create eval-runner-ui \
     --project=<project> \
     --description="Service account for eval-runner UI artifact reads"

   gcloud storage buckets add-iam-policy-binding gs://<bucket> \
     --member="serviceAccount:eval-runner-ui@<project>.iam.gserviceaccount.com" \
     --role="roles/storage.objectViewer"
   ```

2. **Bind the Kubernetes ServiceAccount via Workload Identity:**

   ```bash
   gcloud iam service-accounts add-iam-policy-binding eval-runner-ui@<project>.iam.gserviceaccount.com \
     --project=<project> \
     --role="roles/iam.workloadIdentityUser" \
     --member="serviceAccount:<project>.svc.id.goog[eval-runner/eval-runner-ui]"
   ```

3. **Annotate the Kubernetes ServiceAccount:**

   ```bash
   kubectl -n eval-runner annotate serviceaccount eval-runner-ui \
     iam.gke.io/gcp-service-account=eval-runner-ui@<project>.iam.gserviceaccount.com \
     --overwrite
   ```

> **Note on LocalDir mode:** When running in `artifactDir` mode in Kubernetes, artifacts are written to an `emptyDir` volume attached to the controller pod and are unreadable from the UI pod. GCS is the recommended sink for multi-pod deployments. LocalDir mode is intended for local development and test environments where storage is shared.

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
- `allowedCloneHosts`: Comma-separated allow-list of clone URL hosts for webhook-received pull requests and pushes (default `github.com`; an empty list defaults to `github.com`; wildcards are not supported).
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
- `headTopUpInterval`: Interval for periodic head top-up loop evaluating current branch heads (default `0` = disabled; e.g. `6h`).
- `headTargetCIHalfWidth`: Target 95% confidence interval half-width for branch head scores (default `0.05`). When the pooled CI half-width reaches or falls below this target, top-ups stop for that commit.
- `headMaxSamplesPerTask`: Maximum total samples evaluated per task across all top-up runs at a branch head commit (defaults to `defaultMaxSamplesPerTask`, typically `64`).
- `inconclusivePolicy`: Check-run conclusion policy for INCONCLUSIVE comparisons (`neutral` or `failure`, default `neutral`). When set to `failure`, an INCONCLUSIVE comparison marks the `eval-runner / evals` check run conclusion as `failure` to block PR merging.
- `maxParallelTasks`: Maximum concurrent task sandboxes (default `4`).
- `pollProgressInterval`: Interval between task progress polling attempts and per-task write floor (default `10s`, minimum `1s`).
- `modelSecret`: Name of Secret in sandbox namespace containing model credentials (default `model-credentials`).
- `sandboxdImage`: Container image used for sandboxd init container (defaults to pinned SandboxdImage if unset).
- `taskGracePeriod`: Grace period before failing a Running task whose sandbox is missing (default `30s`).
- `uiBaseURL`: Base URL for the eval-runner UI (e.g. `https://eval-runner.example.com`) to generate absolute links in PR comments.
- `webhookRepositories`: Comma-separated list of allowed repositories (`owner/name`). If unset or empty, all incoming repositories are rejected (fail-closed). Use `*` to explicitly allow all repositories.

Precedence is: explicit CLI flag > ConfigMap key > built-in default. If the ConfigMap is absent, all built-in defaults are used.

### Runtime Configuration Reloading

Both the controller and UI watch `eval-runner-config` and dynamically apply configuration changes at runtime without requiring a deployment restart:
- **Artifact Sink**: When `artifactBucket`, `artifactDir`, or `artifactMaxUploadBytes` is modified (e.g. setting `artifactBucket` after initial cluster deploy), the controller and UI construct the new artifact sink and safely retire the previous one. In-flight uploads and reads are permitted to complete before the old sink is closed. *(Note: Swapping away from the default in-memory sink discards previously stored in-memory artifacts, so Runs or Comparisons in flight at that moment may encounter unreadable artifacts.)*
- **Reloadable Settings**: Changes to deadlines (`discoveryDeadline`, `executionDeadline`), concurrency limits (`maxParallelTasks`), progress polling intervals (`pollProgressInterval`), sandbox task grace periods (`taskGracePeriod`), sandbox images (`sandboxdImage`), model credentials (`modelSecret`), token pricing (`modelPrices`), command authorizers (`commandUsers`), GitHub credentials (`githubTokenSecret`, `githubAppSecret`), PR reporting (`uiBaseURL`, `inconclusivePolicy`), head top-up loop (`headTopUpInterval`, `headTargetCIHalfWidth`, `headMaxSamplesPerTask`), completed comparison retention expiry, and task affinity (`affinityMode`) take effect immediately for subsequent evaluations.
- **Observability**: When settings reload, the controller logs a single `"effective cluster settings"` line reflecting all active values, and the UI increments its internal `config_generation` value and updates the artifact sink status in page footers (`artifacts: gs://bucket`).

#### Restart-Only Settings

Settings that bind to host interfaces, define process identity, or configure webhook startup cannot change at runtime and require restarting the corresponding Deployment or pod:
- **Listen and Probe Addresses**: Controller and UI bind addresses (e.g. `--health-probe-bind-address`, `--listen-addr`).
- **Namespaces**: Target Kubernetes namespaces (e.g. `--namespace`, `--sandbox-namespace`).
- **Webhook Settings**: Keys consumed by `eval-runner-webhook` (`githubWebhookSecret`, `webhookRepositories`, `allowedCloneHosts`, `defaultBatchSize`, `defaultMaxSamplesPerTask`, `defaultModel`) are read once at webhook startup and remain restart-only.

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
   - **Webhook**:
     - Check **Active**
     - **Webhook URL**: `https://<host>/webhook`
     - **Webhook secret**: Generate a random secret and store it in Kubernetes (see step 3).
     - **Subscribe to events**: Check `Issue comment` and `Check run` (`check_run`) to enable interactive review comments and check-run action buttons ("Run 32 more samples", "Re-run baseline", "Re-run candidate"). (The forge poller handles branch pushes and PR synchronization).
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
   kubectl create secret generic github-app \
     --namespace=eval-runner \
     --from-literal=app-id=123456 \
     --from-literal=installation-id=987654 \
     --from-file=private-key=eval-runner.private-key.pem \
     --from-literal=webhook-secret=your-webhook-secret
   ```

   *(Note: The controller and webhook load GitHub App credentials from `github-app`. The webhook uses `webhook-secret` to verify HMAC signatures and the App credentials to check collaborator permissions and update check run states. The private key and tokens live exclusively in the `eval-runner` namespace and are never mounted in sandbox pods.)*

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
   - **Candidate Run**:
     - Single repository allow-list: `pr-<number>-<headSHA7>`
     - Multiple repositories or wildcard (`*`): `pr-<repoHash>-<number>-<headSHA7>`, where `<repoHash>` is a 7-character lowercase hexadecimal SHA-256 hash of the normalized repository name (`owner/repo`), preventing collisions across allow-listed repositories.
     - Evaluates PR head commit with `defaultModel` and initial `sampleRange {1, defaultBatchSize}`.
   - **Baseline Run**:
     - Single repository allow-list: `base-<baseSHA7>`
     - Multiple repositories or wildcard (`*`): `base-<repoHash>-<baseSHA7>`, where `<repoHash>` is a 7-character lowercase hexadecimal SHA-256 hash of the normalized repository name (`owner/repo`), preventing collisions across allow-listed repositories.
     - Evaluates merge target commit with initial `sampleRange {1, defaultBatchSize}`. If an already `Completed` Run with that commit, repo URL, and model exists, it is reused immediately (baseline cache from concept §4.1). Reuse matches on repository URL, commit, and model via labels/spec, never on name alone.
   - **Comparison**:
     - Single repository allow-list: `pr-<number>`
     - Multiple repositories or wildcard (`*`): `pr-<repoHash>-<number>`
     - Links candidate and baseline runs with `spec.pullRequest` and `spec.sampling {batchSize: defaultBatchSize, maxSamplesPerTask: defaultMaxSamplesPerTask}` configured.
   - **Lookups & Backwards Compatibility**: Comparison lookups for synchronization, `/eval more` comment commands, and PR cleanup resolve primarily by labels (`evalrunner.gke-labs.dev/pr` and `evalrunner.gke-labs.dev/repo`), falling back to direct name checks (`pr-<number>` and `pr-<repoHash>-<number>`). Already-created objects from previous deployments continue to resolve and update in-place without collision or duplication.
   - **Redelivery & Synchronize**: Redelivery of the same event is idempotent and makes no duplicate objects. A `synchronize` event creates the new candidate Run `pr-<number>-<newHead7>` (or `pr-<repoHash>-<number>-<newHead7>`), keeps previous candidate Runs in place (they are not deleted, preserving execution history), and updates `spec.candidate.run` and `spec.pullRequest.headSHA` on the existing Comparison.
2. **On `pull_request` (`closed`)**:
   - **Cleanup**: Label-selects the Comparison(s) matching `evalrunner.gke-labs.dev/pr` and `spec.pullRequest.repository` and deletes them; their owned batch Runs cascade via Kubernetes owner references.
   - **Seed Runs**: Deletes seed candidate Runs for the PR head. If a seed Run carries an owner reference to a surviving live Comparison or is referenced as `spec.baseline.run` by another surviving Comparison (e.g. stacked branches or fast-forward merges), it is preserved.
   - **Baseline Runs**: Preserved for reuse across PRs.
   - **Reopen**: If a closed PR is subsequently reopened, resources are recreated on the next `pull_request` event.
   - **Artifacts**: Artifacts stored in the artifact sink (GCS or local directory) are not purged by PR closure.
3. **On `push` to the default branch**:
   - **Warm Baseline Run**: `base-<sha7>` (single repository allow-list) or `base-<repoHash>-<sha7>` (multiple repositories or wildcard `*`) is created with initial `sampleRange {1, defaultBatchSize}` so the baseline run is pre-warmed before PRs arrive.
   - **Branch Label & Commit Time Annotation**: The run is labeled with `evalrunner.gke-labs.dev/branch=<defaultBranch>` and annotated with `evalrunner.gke-labs.dev/commit-time=<RFC3339>`. If configured in ConfigMap key `defaultCohort`, `spec.cohort` is set on the Run.
   - **Main-Branch Series Page**: Powers `/series` and `/series/{repohash}/{branch}?task=<id>&n=50` displaying commit-by-commit success rates via static SVG with confidence interval bands and commit metadata table. Commits are ordered chronologically by commit-time, falling back to `creationTimestamp` with a visible "ordered by creation time" note. Pre-existing Runs lacking the branch label are excluded.
4. **On `issue_comment` (`created`)**:
   - **More Samples Command**: Comments of the form `/eval more <n>` on a pull request trigger evaluation of additional samples.
   - **Credential-Free Webhook**: After verifying HMAC and the repository allow-list, the webhook appends `{commentId, login, requested, receivedAt}` to a bounded `status.pendingCommands` on the Comparison (idempotent per comment ID against pending and handled commands). The webhook holds no GitHub tokens and makes no outbound GitHub calls.
   - **Controller Processing**: The controller asynchronously processes pending commands: it verifies write or admin permission via the GitHub collaborator permissions API or the `commandUsers` allow-list, failing closed on API error. Transient permission API errors and transient Secret read errors are retried with time-gated bounded retry (up to 3 attempts with backoff: 10s, 1m, 5m, or `Retry-After`, recorded in `status.pendingCommands[].attempts` and `nextAttemptAt`), after which errors become terminal. For authorized commands, it clamps and increments `spec.extraSamples` on the Comparison, acknowledges the comment via reaction (`eyes`) or fallback comment reply, and records the outcome in a bounded `status.handledCommands`. Denied or errored commands react with `confused` feedback (no comment replies).
5. **Labels**:
   All created resources are labeled with:
   - `evalrunner.gke-labs.dev/pr`: PR number (for PR runs and comparisons)
   - `evalrunner.gke-labs.dev/target-repo`: Sanitized target repository identifier (e.g. `owner_name`)
   - `evalrunner.gke-labs.dev/repo`: Sanitized repository identifier (e.g. `owner_name`)
   - `evalrunner.gke-labs.dev/head-sha`: Commit SHA
   - `evalrunner.gke-labs.dev/branch`: Default branch name (for push-created baseline Runs)

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

