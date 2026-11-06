# Terminal-Bench on Agent Sandbox with Harbor

This directory contains configuration, deployment manifests, and automation scripts to run [Terminal-Bench 2.0](https://github.com/laude-institute/terminal-bench-2) through the [Harbor](https://github.com/harbor-framework/harbor) evaluation framework against Agent Sandbox on Kubernetes clusters.

Each trial executes inside an Agent Sandbox `SandboxClaim` in the `eval-runner-sandbox` namespace using the `k8s-agent-sandbox-harbor` integration. This manual smoke test verifies cluster prerequisites—image pulls from `ghcr.io`, cluster DNS and public egress for verifiers, and pod exec—prior to controller automation.

Upstream references:
- Upstream example: [`examples/harbor-terminal-bench`](https://github.com/kubernetes-sigs/agent-sandbox/tree/56575736db3c17cacc96072ff93d8fa2505d1864/examples/harbor-terminal-bench) at commit `56575736db3c17cacc96072ff93d8fa2505d1864` (the merge commit of agent-sandbox PR 2126).
- Environment documentation: [`clients/integrations/harbor/README.md`](https://github.com/kubernetes-sigs/agent-sandbox/blob/75dd288fd251d22230a90d22ca0ae3523560703e/clients/integrations/harbor/README.md) pinned at ref `75dd288fd251d22230a90d22ca0ae3523560703e` (agent-sandbox PR 2125; not on main yet).

## Prerequisites and Installation

Requirements:
- Python 3.12 or newer
- [`uv`](https://docs.astral.sh/uv/) package manager
- `kubectl` with access to the cluster

Install Harbor and the Agent Sandbox environment in a single isolated tool environment:

```bash
uv tool install harbor --with "git+https://github.com/kubernetes-sigs/agent-sandbox.git@75dd288fd251d22230a90d22ca0ae3523560703e#subdirectory=clients/integrations/harbor"
```

Because `uv tool install` provides an isolated environment, do not run a separate `pip install` or test imports with a different Python interpreter.

## Cluster Setup (evalrunner-dev on GCE)

The walk-through targets **evalrunner-dev, a kops cluster on GCE** using the kubeconfig at `~/.kube/kops`:

1. **Namespace**: The smoke test runs in `eval-runner-sandbox`. Since 2026-11-03, `eval-runner-sandbox` enforces Pod Security Admission `baseline` (#489), permitting the root containers required by Terminal-Bench verifiers. No new namespace is needed.
2. **RBAC Identity**:
   - Determine your active Kubernetes identity:
     ```bash
     kubectl --kubeconfig ~/.kube/kops auth whoami
     ```
   - An admin kubeconfig with the `system:masters` group (such as a kops admin kubeconfig) bypasses RBAC entirely and does not require a RoleBinding.
   - For non-`system:masters` credentials, edit `deploy/rbac.yaml` to replace `REPLACE_WITH_USER` with the subject returned by `kubectl auth whoami` (e.g., your user principal or ServiceAccount name; do not use `admin`).
   - The Role in `deploy/rbac.yaml` grants least-privilege verbs in `eval-runner-sandbox`: create/get/list/watch/delete on `SandboxTemplate`, `SandboxWarmPool`, and `SandboxClaim`; get/list/watch on `sandboxes`; get/list on `pods`; and get/create on `pods/exec`.

### Kind Alternative

To run against a local `kind` cluster, execute `make deploy-kind` **from a checkout of kubernetes-sigs/agent-sandbox at the pinned ref** (`75dd288fd251d22230a90d22ca0ae3523560703e`)—this repository has no Makefile. Note that Terminal-Bench sample images are `linux/amd64` only; on Apple Silicon (arm64), kind runs them under emulation, where nested virtualization is unavailable and QEMU tasks will fail.

## Running the Smoke Test

Execute the 10-task oracle smoke run using `scripts/run-sample.sh`:

```bash
KUBECONFIG=~/.kube/kops examples/terminal-bench/scripts/run-sample.sh
```

The script applies `deploy/rbac.yaml`, runs `harbor run -c configs/sample.yaml`, checks for leftover claims, and prints the report path.

### How a Trial Works

1. **Start**: The environment renders pod specifications for each task and ensures a `SandboxTemplate` and `SandboxWarmPool` exist in `eval-runner-sandbox`. It creates a `SandboxClaim` and waits for it to become Ready and accept exec connections.
2. **Directories**: Sets up `/logs/agent`, `/logs/verifier`, `/logs/artifacts`, `/tests`, and `/solution`.
3. **Agent**: The oracle agent uploads `solution/` and executes `solve.sh`.
4. **Verifier**: Uploads `tests/` and executes `test.sh`, which installs `curl` and `uv` to run pytest. This step relies on cluster DNS and public egress.
5. **Stop**: The `SandboxClaim` is deleted, and its pod is terminated.

While trials run, monitor cluster resources:

```bash
kubectl --kubeconfig ~/.kube/kops -n eval-runner-sandbox get sandboxclaims,sandboxwarmpools,sandboxtemplates
kubectl --kubeconfig ~/.kube/kops -n eval-runner-sandbox get pods -w
```

## Reading the Report and Expected Outcome

Job results are written to `jobs/harbor-terminal-bench/<job_name>/` (or the directory specified via `--jobs-dir` / `-o`). Each trial directory contains `result.json` recording `reward`, timings, and any exception.

- `pass`: Reward of `1.0`.
- `fail`: Reward `< 1.0`.
- `error`: Infrastructure or harness failure (an exception recorded by Harbor).

**Expected Outcome**: Upstream recorded **7 of 10 passing with 0 infrastructure errors**.

Zero infrastructure errors confirms that image pulls from `ghcr.io`, cluster DNS, egress networking, and the pod exec WebSocket path are functioning correctly. The three zero-reward trials are task-side failures:
- `qemu-startup` and `qemu-alpine-ssh`: The verifier's `apt-get install curl` fails due to transient 404 errors from `deb.debian.org` for `bullseye-security` packages, preventing `uv` and pytest from installing.
- `build-cython-ext`: 10 of 11 verifier tests pass; the final test executes the upstream repository test suite which fails inside the image.

## Cleanup

To remove leftover claims, pools, and templates labeled with `harbor.io/managed-by=k8s-agent-sandbox-harbor`:

```bash
KUBECONFIG=~/.kube/kops examples/terminal-bench/scripts/cleanup.sh
```

Pass `--claims` to remove only claims while preserving warm pools and templates.

## Troubleshooting

- **DNS or egress failures**: If verifiers fail with `Could not resolve host` or package download errors, check template egress rules and ensure DNS pods in `kube-system` are reachable.
- **Pod Security Admission**: Benchmark containers execute as root. If pods are rejected, ensure they run in `eval-runner-sandbox` (PSA `baseline`) rather than `eval-runner` (PSA `restricted`).
- **Slow initial trials**: The first trial for each task pulls the container image from `ghcr.io` (images range up to 1.3 GB for `qemu-alpine-ssh`). `ready_timeout_s` is set to 600 seconds to allow cold pulls.
- **Interrupted runs**: If interrupted via `Ctrl-C`, invoke `scripts/cleanup.sh` to delete lingering sandbox objects.

## Limits

- Harbor stream mode, Docker Compose tasks, GPU/TPU tasks, and network allowlists are not supported by the integration.
- Tasks must provide prebuilt container images (all Terminal-Bench 2.0 tasks supply images on `ghcr.io`).
