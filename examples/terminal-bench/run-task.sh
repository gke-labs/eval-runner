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

set -o errexit
set -o nounset
set -o pipefail

unset GIT_CONFIG_GLOBAL GIT_CONFIG_SYSTEM

task_name="${1:-}"
mode="${2:-oracle}"
task_workdir="${3:-${TASK_WORKDIR:-/app}}"

if [ -z "${task_name}" ]; then
  echo "Usage: run-task.sh <task-name> [oracle|agent] [workdir]" >&2
  exit 1
fi

LOGS_DIRECTORY="${LOGS_DIRECTORY:-/logs}"
OUT_DIRECTORY="${OUT_DIRECTORY:-/out}"
SOLUTION_DIRECTORY="${SOLUTION_DIRECTORY:-/solution}"
TESTS_DIRECTORY="${TESTS_DIRECTORY:-/tests}"

mkdir -p "${LOGS_DIRECTORY}/agent" "${LOGS_DIRECTORY}/verifier" "${LOGS_DIRECTORY}/artifacts" "${OUT_DIRECTORY}/logs" "${SOLUTION_DIRECTORY}" "${TESTS_DIRECTORY}"

write_results_and_exit() {
  local passed="$1"
  local score="$2"
  local raw_message="${3:-}"

  local escaped_message
  escaped_message="$(printf '%s' "${raw_message}" | awk '{
    gsub(/[\001-\010\013\014\016-\037\177]/, "");
    gsub(/\\/, "\\\\");
    gsub(/"/, "\\\"");
    gsub(/\r/, "\\r");
    gsub(/\t/, "\\t");
    if (NR > 1) {
      printf("\\n");
    }
    printf("%s", $0);
  }')"

  cat <<EOF > "${OUT_DIRECTORY}/results.json"
{
  "schemaVersion": 1,
  "samples": [
    {
      "id": "${task_name}",
      "passed": ${passed},
      "score": ${score},
      "message": "${escaped_message}"
    }
  ]
}
EOF
  exit 0
}

pinned_commit="${PINNED_COMMIT:-}"
if [ -z "${pinned_commit}" ]; then
  script_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  if [ -f "${script_directory}/version.sh" ]; then
    # shellcheck source=/dev/null
    source "${script_directory}/version.sh"
  elif [ -f "examples/terminal-bench/version.sh" ]; then
    # shellcheck source=/dev/null
    source "examples/terminal-bench/version.sh"
  fi
  pinned_commit="${PINNED_COMMIT:-}"
fi

if [ -z "${pinned_commit}" ]; then
  write_results_and_exit false 0.0 "PINNED_COMMIT not set: version.sh not found"
fi

task_source_directory=""
if [ -n "${TASK_DIRECTORY:-}" ]; then
  task_source_directory="${TASK_DIRECTORY}/${task_name}"
fi

if [ -z "${task_source_directory}" ]; then
  fetch_log="${OUT_DIRECTORY}/logs/fetch.log"
  fetch_checkout_directory="/tmp/terminal-bench"
  rm -rf "${fetch_checkout_directory}"
  mkdir -p "${fetch_checkout_directory}"

  if ! (
    echo "=== Fetching terminal-bench repository at ${pinned_commit} ==="
    if ! command -v git >/dev/null 2>&1; then
      if command -v apt-get >/dev/null 2>&1; then
        apt-get update && apt-get install -y git
      elif command -v apk >/dev/null 2>&1; then
        apk add --no-cache git
      fi
    fi

    if command -v git >/dev/null 2>&1; then
      cd "${fetch_checkout_directory}"
      git init
      git remote add origin "https://github.com/laude-institute/terminal-bench-2.git"
      git fetch --depth 1 origin "${pinned_commit}"
      git checkout FETCH_HEAD
    elif command -v curl >/dev/null 2>&1; then
      curl -LsSf "https://github.com/laude-institute/terminal-bench-2/archive/${pinned_commit}.tar.gz" | tar -xz -C "${fetch_checkout_directory}" --strip-components=1
    elif command -v wget >/dev/null 2>&1; then
      wget -qO- "https://github.com/laude-institute/terminal-bench-2/archive/${pinned_commit}.tar.gz" | tar -xz -C "${fetch_checkout_directory}" --strip-components=1
    elif command -v python3 >/dev/null 2>&1; then
      python3 -c "import urllib.request, tarfile, io; r = urllib.request.urlopen('https://github.com/laude-institute/terminal-bench-2/archive/${pinned_commit}.tar.gz'); tarfile.open(fileobj=io.BytesIO(r.read()), mode='r:gz').extractall('${fetch_checkout_directory}')" && \
      if [ -d "${fetch_checkout_directory}/terminal-bench-2-${pinned_commit}" ]; then
        cp -a "${fetch_checkout_directory}/terminal-bench-2-${pinned_commit}/." "${fetch_checkout_directory}/"
      fi && test -d "${fetch_checkout_directory}/${task_name}"
    else
      echo "ERROR: no tool available to fetch repository (need git, curl, wget, or python3)"
      false
    fi
  ) > "${fetch_log}" 2>&1; then
    write_results_and_exit false 0.0 "failed to fetch repository: $(tail -n 20 "${fetch_log}" 2>/dev/null)"
  fi

  task_source_directory="${fetch_checkout_directory}/${task_name}"
fi

if [ ! -d "${task_source_directory}" ]; then
  write_results_and_exit false 0.0 "task directory not found: ${task_name}"
fi

if [ -d "${task_source_directory}/solution" ]; then
  rm -rf "${SOLUTION_DIRECTORY:?}"/*
  cp -a "${task_source_directory}/solution/." "${SOLUTION_DIRECTORY}/"
  chmod -R +x "${SOLUTION_DIRECTORY}" 2>/dev/null || true
fi

if [ -d "${task_source_directory}/tests" ]; then
  rm -rf "${TESTS_DIRECTORY:?}"/*
  cp -a "${task_source_directory}/tests/." "${TESTS_DIRECTORY}/"
  chmod -R +x "${TESTS_DIRECTORY}" 2>/dev/null || true
fi

if [ -n "${task_workdir}" ]; then
  mkdir -p "${task_workdir}"
  cd "${task_workdir}"
fi

solve_log="${OUT_DIRECTORY}/logs/solve.log"
if [ "${mode}" = "oracle" ] && [ -f "${SOLUTION_DIRECTORY}/solve.sh" ]; then
  bash "${SOLUTION_DIRECTORY}/solve.sh" > "${solve_log}" 2>&1 || true
fi

verifier_log="${OUT_DIRECTORY}/logs/verifier.log"
if [ -f "${TESTS_DIRECTORY}/test.sh" ]; then
  bash "${TESTS_DIRECTORY}/test.sh" > "${verifier_log}" 2>&1 || true
else
  printf "test script not found at %s/test.sh\n" "${TESTS_DIRECTORY}" > "${verifier_log}"
fi

reward_json_file="${LOGS_DIRECTORY}/verifier/reward.json"
reward_text_file="${LOGS_DIRECTORY}/verifier/reward.txt"
raw_reward=""

if [ -f "${reward_json_file}" ]; then
  raw_reward="$(awk '
    /"reward"[[:space:]]*:[[:space:]]*/ {
      match_index = match($0, /"reward"[[:space:]]*:[[:space:]]*[0-9.]+/)
      if (match_index > 0) {
        match_text = substr($0, match_index, RLENGTH)
        split(match_text, fields, ":")
        gsub(/[^0-9.]/, "", fields[2])
        if (fields[2] != "") {
          print fields[2]
          exit
        }
      }
    }
    BEGIN { value = "" }
    {
      gsub(/[^0-9.]/, "", $0)
      if ($0 != "") value = $0
    }
    END { if (value != "") print value }
  ' "${reward_json_file}" | tr -d '[:space:]')"
elif [ -f "${reward_text_file}" ]; then
  raw_reward="$(cat "${reward_text_file}" 2>/dev/null | tr -d '[:space:]')"
else
  write_results_and_exit false 0.0 "verifier did not write reward file: $(tail -n 20 "${verifier_log}" 2>/dev/null)"
fi

if [ -z "${raw_reward}" ] || [[ ! "${raw_reward}" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
  write_results_and_exit false 0.0 "non-numeric reward '${raw_reward}': $(tail -n 20 "${verifier_log}" 2>/dev/null)"
fi

if [ "$(awk -v r="${raw_reward}" 'BEGIN { if (r + 0 == 1) print 1; else print 0 }')" = "1" ]; then
  write_results_and_exit true 1.0 ""
else
  score="$(awk -v r="${raw_reward}" 'BEGIN { printf "%.2f", r + 0 }')"
  write_results_and_exit false "${score}" "$(tail -n 20 "${verifier_log}" 2>/dev/null)"
fi
