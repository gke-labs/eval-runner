#!/bin/bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Adapted from kubernetes-sigs/agent-sandbox examples/harbor-terminal-bench (Apache-2.0)

# Smoke test: the oracle agent on the Terminal-Bench 2.0 sample.
# Every trial must score 1.0.
#
# Usage:
#   examples/terminal-bench/scripts/run-sample.sh [extra harbor run flags]
#
# Environment variables:
#   NAMESPACE: Kubernetes namespace (default: eval-runner-sandbox).
#   KUBE_CONTEXT: kubectl context (default: current context).

set -euo pipefail

script_directory="$(cd "$(dirname "$0")" && pwd)"
base_directory="$(cd "${script_directory}/.." && pwd)"
namespace="${NAMESPACE:-eval-runner-sandbox}"

kubectl_command=(kubectl)
if [[ -n "${KUBE_CONTEXT:-}" ]]; then
  kubectl_command+=(--context "${KUBE_CONTEXT}")
fi

"${kubectl_command[@]}" apply -f "${base_directory}/deploy/rbac.yaml"

if ! command -v harbor >/dev/null 2>&1; then
  echo "harbor is not on PATH. Install it using:"
  echo '  uv tool install harbor --with "git+https://github.com/kubernetes-sigs/agent-sandbox.git@75dd288fd251d22230a90d22ca0ae3523560703e#subdirectory=clients/integrations/harbor"'
  exit 1
fi

job_name="sample-$(date +%Y%m%d-%H%M%S)-$$"
jobs_directory="jobs/harbor-terminal-bench"
argument_list=()
if [[ $# -gt 0 ]]; then
  argument_list=("$@")
fi

for ((index = 0; index < ${#argument_list[@]}; index++)); do
  case "${argument_list[${index}]}" in
    --job-name)
      job_name="${argument_list[$((index + 1))]:-${job_name}}"
      ;;
    --job-name=*)
      job_name="${argument_list[${index}]#--job-name=}"
      ;;
    --jobs-dir|-o)
      jobs_directory="${argument_list[$((index + 1))]:-${jobs_directory}}"
      ;;
    --jobs-dir=*)
      jobs_directory="${argument_list[${index}]#--jobs-dir=}"
      ;;
    -o?*)
      jobs_directory="${argument_list[${index}]#-o}"
      ;;
  esac
done

echo "== harbor run: terminal-bench-sample@2.0, oracle agent, namespace ${namespace}, job ${job_name}"
harbor_command=(harbor run -c "${base_directory}/configs/sample.yaml" --job-name "${job_name}" --ek "namespace=${namespace}")
if [[ -n "${KUBE_CONTEXT:-}" ]]; then
  harbor_command+=(--ek "kube_context=${KUBE_CONTEXT}")
fi
if [[ $# -gt 0 ]]; then
  harbor_command+=("$@")
fi

"${harbor_command[@]}"

echo "== leftovers in ${namespace} (expect none):"
"${kubectl_command[@]}" -n "${namespace}" get sandboxclaims 2>/dev/null || true

echo "== report path:"
echo "${jobs_directory}/${job_name}"
