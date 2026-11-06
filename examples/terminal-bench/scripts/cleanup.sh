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

# Delete leftover Agent Sandbox objects created by Harbor in a namespace.
#
# Usage:
#   examples/terminal-bench/scripts/cleanup.sh [namespace] [--claims|--all]
#
# Environment variables:
#   NAMESPACE: Kubernetes namespace (default: eval-runner-sandbox).
#   KUBE_CONTEXT: kubectl context (default: current context).

set -euo pipefail

namespace="${NAMESPACE:-eval-runner-sandbox}"
target_resources="sandboxclaims,sandboxwarmpools,sandboxtemplates"

if [[ $# -gt 0 ]]; then
  for argument in "$@"; do
    case "${argument}" in
      --claims)
        target_resources="sandboxclaims"
        ;;
      --all)
        target_resources="sandboxclaims,sandboxwarmpools,sandboxtemplates"
        ;;
      *)
        namespace="${argument}"
        ;;
    esac
  done
fi

kubectl_command=(kubectl)
if [[ -n "${KUBE_CONTEXT:-}" ]]; then
  kubectl_command+=(--context "${KUBE_CONTEXT}")
fi

echo "Removing ${target_resources} in namespace ${namespace} with selector harbor.io/managed-by=k8s-agent-sandbox-harbor..."
"${kubectl_command[@]}" -n "${namespace}" delete "${target_resources}" -l "harbor.io/managed-by=k8s-agent-sandbox-harbor" --ignore-not-found=true
