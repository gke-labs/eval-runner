# Terminal-Bench 2.0 Example

This example demonstrates running Terminal-Bench 2.0 evaluation tasks natively as sandbox tasks within `eval-runner` from the real benchmark repository (`laude-institute/terminal-bench-2`).

Each task executes in its dedicated container sandbox using the prebuilt Docker Hub task image from `alexgshaw/<name>:20251031` with the repository checked out at the Run's commit. The sandbox task executes as the container image's user (root for Terminal-Bench images) under Kubernetes baseline Pod Security Admission, bounded by per-task `timeoutSeconds`.

Note: Terminal-Bench 2.0 container images are prebuilt for `linux/amd64` only. Cluster worker nodes must run amd64 architecture.

## How the List is Generated

The task manifest in `examples/terminal-bench/.evals/list` is generated dynamically at discovery time. When discovery runs in a sandbox pod with network egress, `.evals/list` shallow-fetches `laude-institute/terminal-bench-2` at the pinned commit into a temporary directory and emits all 89 tasks. For each task directory, it reads:
- `docker_image`, `cpus`, and `memory` from `task.toml`
- `agent.timeout_sec` and `verifier.timeout_sec` from `task.toml` (`timeoutSeconds` is their sum, capped at the controller maximum of 7200 seconds)
- The working directory from the last `WORKDIR` directive in `environment/Dockerfile` (defaulting to `/app`)

The emitted `metadata.cpus`/`metadata.memory` is informational today because `ManifestTask` has no such field and `ParseManifest` drops unknown keys; the per-task-resources slice must add the field before anything consumes it.

## Pinned Commit and How to Bump It

The benchmark repository commit is pinned in `examples/terminal-bench/version.sh`:

```sh
PINNED_COMMIT="69671fbaac6d67a7ef0dfec016cc38a64ef7a77c"
```

To bump the benchmark version across both discovery and execution, update `PINNED_COMMIT` in `examples/terminal-bench/version.sh`.

## Curated Subset in Root `.evals/list`

The root `.evals/list` includes a curated subset of 4 lightweight tasks for dogfooding on development clusters:
- `terminal-bench/regex-log`
- `terminal-bench/fix-code-vulnerability`
- `terminal-bench/log-summary-date-ranges`
- `terminal-bench/polyglot-c-py`

Each entry carries `paths: ["examples/terminal-bench/**"]` so changes to this directory trigger evaluation.

## Resource Limits Caveat

Tasks execute under default sandbox container resource limits (500m CPU, 512Mi memory). While lightweight tasks run reliably within these bounds, heavier tasks whose `task.toml` requests additional CPU or memory (such as `qemu-alpine-ssh`, `qemu-startup`, or `sqlite-with-gcov`) still appear in the 89-task manifest but will fail until per-task container resource plumbing is implemented in a follow-up slice.

## Running on a Cluster

To evaluate the `terminal-bench/regex-log` task against this repository on your cluster, specify a valid 7–64 character git commit hash for `spec.repo.commit`:

```yaml
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: terminal-bench-regex-log-run
  namespace: eval-runner
spec:
  repo:
    url: https://github.com/gke-labs/eval-runner.git
    commit: 0123456789abcdef0123456789abcdef01234567
  tasks:
  - terminal-bench/regex-log
```

Apply the Run manifest:

```bash
kubectl apply -f run.yaml
```

Inspect progress and outcomes using `kubectl`:

```bash
kubectl get run terminal-bench-regex-log-run -n eval-runner -o yaml
```

## E2E Testing Notes

In `images/git-server/Dockerfile`, `sed` removes `terminal-bench` entries from the `docs-eval.git` fixture's `.evals/list` so existing comprehension and comparison tests remain focused solely on documentation tasks. The `terminal-bench.git` e2e test fixture repository duplicates `examples/terminal-bench/` at both the repository root (to serve `.evals/list` during discovery) and nested under `examples/terminal-bench/` (to match the task command path).
