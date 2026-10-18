// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ui

import (
	"html/template"
	"io"
)

const baseCSS = `
:root {
  --indigo: #4F46E5;
  --indigo-soft: #EEF2FF;
  --teal: #14B8A6;
  --teal-soft: #CCFBF1;
  --ink: #111827;
  --ink-2: #374151;
  --muted: #6B7280;
  --line: #E5E7EB;
  --surface: #FFFFFF;
  --canvas: #F8FAFC;
  --good: #15803D;
  --good-soft: #DCFCE7;
  --bad: #B91C1C;
  --bad-soft: #FEE2E2;
  --warn: #B45309;
  --warn-soft: #FEF3C7;
  --neutral: #6B7280;
  --neutral-soft: #F3F4F6;
  --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  --mono: "SF Mono", Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
}

* { box-sizing: border-box; }
body {
  margin: 0;
  padding: 0;
  font-family: var(--sans);
  color: var(--ink);
  background: var(--canvas);
  -webkit-font-smoothing: antialiased;
  min-height: 100vh;
}
a { color: var(--indigo); text-decoration: none; }
a:hover { text-decoration: underline; }

.header-bar {
  background: var(--surface);
  border-bottom: 1px solid var(--line);
  padding: 14px 28px;
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.header-brand {
  display: flex;
  align-items: center;
  gap: 24px;
}
.header-logo {
  font-weight: 700;
  font-size: 20px;
  color: var(--indigo);
  text-decoration: none;
  display: flex;
  align-items: center;
  gap: 8px;
}
.header-logo:hover { text-decoration: none; }
.nav-links {
  display: flex;
  gap: 8px;
}
.nav-link {
  font-size: 14px;
  font-weight: 500;
  color: var(--muted);
  padding: 6px 14px;
  border-radius: 6px;
  text-decoration: none;
  transition: all 0.15s ease;
}
.nav-link:hover {
  color: var(--ink);
  background: var(--neutral-soft);
  text-decoration: none;
}
.nav-link.active {
  color: var(--indigo);
  background: var(--indigo-soft);
  font-weight: 600;
}

.container {
  max-width: 1320px;
  margin: 28px auto;
  padding: 0 24px;
}

.card {
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: 12px;
  box-shadow: 0 2px 6px rgba(17,24,39,.04);
  overflow: hidden;
  margin-bottom: 24px;
}
.card-header {
  padding: 18px 24px;
  border-bottom: 1px solid var(--line);
  background: var(--surface);
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 12px;
}
.card-title {
  font-size: 20px;
  font-weight: 600;
  margin: 0;
  display: flex;
  align-items: center;
  gap: 12px;
}
.card-body {
  padding: 24px;
}
.card-footer {
  padding: 16px 24px;
  background: var(--canvas);
  border-top: 1px solid var(--line);
  color: var(--muted);
  font-size: 14px;
  display: flex;
  flex-wrap: wrap;
  gap: 28px;
  align-items: center;
}

.mono { font-family: var(--mono); }
.num { font-family: var(--mono); font-variant-numeric: tabular-nums; }

.chip {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 3px 10px;
  border-radius: 999px;
  font-size: 13px;
  font-weight: 600;
  line-height: 1.2;
}
.chip.good { background: var(--good-soft); color: var(--good); }
.chip.bad { background: var(--bad-soft); color: var(--bad); }
.chip.warn { background: var(--warn-soft); color: var(--warn); }
.chip.neutral { background: var(--neutral-soft); color: var(--neutral); }
.chip.pending { background: var(--warn-soft); color: var(--warn); }
.dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: currentColor;
  display: inline-block;
}

/* Confidence-interval bar against zero line (Comparison) */
.cibar { position: relative; height: 26px; width: 340px; }
.cibar .axis { position: absolute; left: 0; right: 0; top: 12px; height: 2px; background: var(--line); }
.cibar .zero { position: absolute; top: 2px; width: 2px; height: 22px; background: var(--muted); }
.cibar .range { position: absolute; top: 11px; height: 4px; border-radius: 2px; }
.cibar .pt { position: absolute; top: 6px; width: 14px; height: 14px; border-radius: 50%; border: 2px solid white; box-sizing: border-box; }
.good-c { background: var(--good); }
.bad-c { background: var(--bad); }
.warn-c { background: var(--warn); }
.neutral-c { background: var(--neutral); }
.indigo-c { background: var(--indigo); }
.teal-c { background: var(--teal); }

/* Confidence-interval bar against 0-1 axis (Run) */
.cibar-01 { position: relative; height: 26px; width: 240px; }
.cibar-01 .axis { position: absolute; left: 0; right: 0; top: 12px; height: 2px; background: var(--line); }
.cibar-01 .tick-0 { position: absolute; left: 0; top: 5px; width: 1px; height: 15px; background: var(--muted); }
.cibar-01 .tick-50 { position: absolute; left: 120px; top: 8px; width: 1px; height: 10px; background: var(--line); }
.cibar-01 .tick-100 { position: absolute; right: 0; top: 5px; width: 1px; height: 15px; background: var(--muted); }
.cibar-01 .range { position: absolute; top: 11px; height: 4px; border-radius: 2px; background: var(--indigo); }
.cibar-01 .pt { position: absolute; top: 6px; width: 14px; height: 14px; border-radius: 50%; border: 2px solid white; box-sizing: border-box; background: var(--indigo); }

table.ci { width: 100%; border-collapse: collapse; font-size: 15px; }
table.ci th { text-align: left; color: var(--muted); font-weight: 500; font-size: 13px; padding: 10px 14px; border-bottom: 1px solid var(--line); }
table.ci td { padding: 12px 14px; border-bottom: 1px solid var(--line); vertical-align: middle; }
table.ci tbody tr:hover { background: #FAFBFD; }
table.ci tbody tr:last-child td { border-bottom: none; }

.verdict { font-size: 20px; margin: 4px 0 20px; display: flex; align-items: center; gap: 14px; flex-wrap: wrap; }
.verdict-headline { font-size: 17px; color: var(--ink-2); }

.meta-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 14px; margin-bottom: 22px; }
.meta-tile { border: 1px solid var(--line); border-radius: 8px; padding: 12px 16px; background: var(--canvas); }
.meta-tile .label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 4px; }
.meta-tile .value { font-size: 14px; font-weight: 600; color: var(--ink); word-break: break-all; }

.empty-state { text-align: center; padding: 48px 16px; color: var(--muted); font-size: 15px; }
.log-path { font-size: 12px; background: var(--neutral-soft); padding: 2px 6px; border-radius: 4px; display: inline-block; max-width: 320px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.msg-alert { margin-top: 14px; padding: 10px 14px; background: var(--bad-soft); color: var(--bad); border-radius: 6px; font-size: 14px; }
`

const runsListTemplate = `
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Runs · eval-runner</title>
  <style>` + baseCSS + `</style>
</head>
<body>
  <header class="header-bar">
    <div class="header-brand">
      <a href="/runs" class="header-logo">eval-runner</a>
      <nav class="nav-links">
        <a href="/runs" class="nav-link {{ if eq .ActiveTab "runs" }}active{{ end }}">Runs</a>
        <a href="/comparisons" class="nav-link {{ if eq .ActiveTab "comparisons" }}active{{ end }}">Comparisons</a>
      </nav>
    </div>
    <div style="font-size:13px;color:var(--muted)">namespace: <span class="mono">{{ .Namespace }}</span></div>
  </header>

  <main class="container">
    <div class="card">
      <div class="card-header">
        <h1 class="card-title">Runs</h1>
        <span style="font-size:13px;color:var(--muted)">{{ .TotalCount }} total</span>
      </div>
      <div style="overflow-x:auto">
        <table class="ci">
          <thead>
            <tr>
              <th>Run</th>
              <th>Namespace</th>
              <th>Phase</th>
              <th>Tasks</th>
              <th>Samples</th>
              <th>Rates</th>
              <th>Commit</th>
              <th>Age</th>
            </tr>
          </thead>
          <tbody>
            {{ range .Runs }}
            <tr>
              <td><a href="{{ .Link }}" class="mono" style="font-weight:600">{{ .Name }}</a></td>
              <td><span class="mono" style="font-size:13px">{{ .Namespace }}</span></td>
              <td><span class="chip {{ .PhaseChip }}"><span class="dot"></span>{{ .Phase }}</span></td>
              <td class="num">{{ .Tasks }}</td>
              <td class="num">{{ if .SampleRange }}{{ .SampleRange }}{{ else }}{{ .Samples }}{{ end }}</td>
              <td class="mono" style="font-size:13px">{{ if .Rates }}{{ .Rates }}{{ else }}—{{ end }}</td>
              <td><span class="mono" style="font-size:13px">{{ if .Commit }}{{ .Commit }}{{ else }}—{{ end }}</span></td>
              <td style="color:var(--muted);font-size:13px">{{ .Age }}</td>
            </tr>
            {{ else }}
            <tr>
              <td colspan="8" class="empty-state">No Runs found in namespace {{ .Namespace }}.</td>
            </tr>
            {{ end }}
          </tbody>
        </table>
      </div>
      {{ if or .HasPrev .HasMore }}
      <div class="pagination" style="padding:14px 24px;border-top:1px solid var(--line);display:flex;justify-content:space-between;align-items:center;font-size:13px">
        <div>
          {{ if .HasPrev }}<a href="{{ .PrevLink }}">← Previous</a>{{ end }}
        </div>
        <div style="color:var(--muted)">{{ .ShowingRange }}</div>
        <div>
          {{ if .HasMore }}<a href="{{ .NextLink }}">More →</a>{{ end }}
        </div>
      </div>
      {{ end }}
    </div>
  </main>
</body>
</html>
`

const runDetailTemplate = `
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ .Name }} · Run · eval-runner</title>
  <style>` + baseCSS + `</style>
</head>
<body>
  <header class="header-bar">
    <div class="header-brand">
      <a href="/runs" class="header-logo">eval-runner</a>
      <nav class="nav-links">
        <a href="/runs" class="nav-link {{ if eq .ActiveTab "runs" }}active{{ end }}">Runs</a>
        <a href="/comparisons" class="nav-link {{ if eq .ActiveTab "comparisons" }}active{{ end }}">Comparisons</a>
      </nav>
    </div>
    <div style="font-size:13px;color:var(--muted)">namespace: <span class="mono">{{ .Namespace }}</span></div>
  </header>

  <main class="container">
    <div class="card">
      <div class="card-header">
        <h1 class="card-title">
          <span class="mono">{{ .Name }}</span>
          <span class="chip {{ .PhaseChip }}"><span class="dot"></span>{{ .Phase }}</span>
        </h1>
        <div style="font-size:13px;color:var(--muted)"><a href="/runs">← Back to Runs</a></div>
      </div>
      <div class="card-body">
        <div class="meta-grid">
          <div class="meta-tile">
            <div class="label">Repository</div>
            <div class="value">{{ if .RepoURL }}{{ .RepoURL }}{{ else }}—{{ end }}</div>
          </div>
          <div class="meta-tile">
            <div class="label">Commit</div>
            <div class="value mono">{{ if .CommitShort }}{{ .CommitShort }}{{ else }}—{{ end }}</div>
          </div>
          <div class="meta-tile">
            <div class="label">Model</div>
            <div class="value mono">{{ if .Model }}{{ .Model }}{{ else }}—{{ end }}</div>
          </div>
          <div class="meta-tile">
            <div class="label">Age</div>
            <div class="value">{{ .Age }}</div>
          </div>
        </div>

        <div class="meta-grid">
          <div class="meta-tile">
            <div class="label">Tasks</div>
            <div class="value">{{ if .TasksSummary }}{{ .TasksSummary }}{{ else }}0/0{{ end }} ({{ .TasksCompleted }} ok, {{ .TasksFailed }} fail)</div>
          </div>
          <div class="meta-tile">
            <div class="label">Total Samples</div>
            <div class="value num">{{ .TotalSamples }}{{ if .SampleRange }} <span style="font-size:13px;font-weight:normal;color:var(--muted)">({{ .SampleRange }})</span>{{ end }}</div>
          </div>
          <div class="meta-tile">
            <div class="label">Mean Success Rate</div>
            <div class="value">{{ if .MeanSuccessRate }}{{ .MeanSuccessRate }}{{ else }}—{{ end }}</div>
          </div>
          <div class="meta-tile">
            <div class="label">Artifacts</div>
            <div class="value mono" style="font-size:12px">{{ if .ArtifactsPrefix }}<span class="log-path" title="{{ .ArtifactsPrefix }}">{{ .ArtifactsPrefix }}</span>{{ else }}—{{ end }}</div>
          </div>
        </div>

        {{ if .Message }}
        <div class="msg-alert"><b>Message:</b> {{ .Message }}</div>
        {{ end }}

        <h2 style="font-size:17px;font-weight:600;margin:24px 0 12px">Eval Tasks</h2>
        <div style="overflow-x:auto">
          <table class="ci">
            <thead>
              <tr>
                <th>eval</th>
                <th>phase</th>
                <th>rate</th>
                <th colspan="2">95% CI (0-1 axis)</th>
                <th>n</th>
                <th>method</th>
                <th>log path</th>
              </tr>
            </thead>
            <tbody>
              {{ range .Tasks }}
              <tr>
                <td class="mono" style="font-weight:600">{{ .ID }}</td>
                <td><span class="chip {{ .PhaseChip }}"><span class="dot"></span>{{ .Phase }}</span></td>
                <td class="num">{{ .RateStr }}</td>
                <td class="num" style="font-size:13px">{{ if .HasCI }}{{ .CILabel }}{{ else }}—{{ end }}</td>
                <td>
                  {{ if .HasCI }}
                  <div class="cibar-01" title="Rate: {{ .RateStr }} {{ .CILabel }}">
                    <div class="axis"></div>
                    <div class="tick-0"></div>
                    <div class="tick-50"></div>
                    <div class="tick-100"></div>
                    <div class="range" style="left:{{ printf "%.1f" .CILeftPx }}px; width:{{ printf "%.1f" .CIWidthPx }}px"></div>
                    <div class="pt" style="left:{{ printf "%.1f" .CIDotPx }}px"></div>
                  </div>
                  {{ else }}
                  <span style="color:var(--muted)">—</span>
                  {{ end }}
                </td>
                <td class="num">{{ if gt .Samples 0 }}{{ .Successes }}/{{ .Samples }}{{ if .SampleRange }} <span style="font-size:11px;color:var(--muted)">({{ .SampleRange }})</span>{{ end }}{{ else }}—{{ end }}</td>
                <td class="mono" style="font-size:13px">{{ if .Method }}{{ .Method }}{{ else }}—{{ end }}</td>
                <td>
                  {{ if .LogPath }}
                  <span class="log-path mono" title="{{ .LogPath }}">{{ .LogPath }}</span>
                  {{ else }}
                  <span style="color:var(--muted)">—</span>
                  {{ end }}
                </td>
              </tr>
              {{ if .Message }}
              <tr>
                <td colspan="8" style="padding-top:0;padding-bottom:12px;color:var(--muted);font-size:12px">
                  <b>Detail:</b> {{ .Message }}
                </td>
              </tr>
              {{ end }}
              {{ else }}
              <tr>
                <td colspan="8" class="empty-state">No task executions recorded for this run.</td>
              </tr>
              {{ end }}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </main>
</body>
</html>
`

const comparisonsListTemplate = `
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Comparisons · eval-runner</title>
  <style>` + baseCSS + `</style>
</head>
<body>
  <header class="header-bar">
    <div class="header-brand">
      <a href="/runs" class="header-logo">eval-runner</a>
      <nav class="nav-links">
        <a href="/runs" class="nav-link {{ if eq .ActiveTab "runs" }}active{{ end }}">Runs</a>
        <a href="/comparisons" class="nav-link {{ if eq .ActiveTab "comparisons" }}active{{ end }}">Comparisons</a>
      </nav>
    </div>
    <div style="font-size:13px;color:var(--muted)">namespace: <span class="mono">{{ .Namespace }}</span></div>
  </header>

  <main class="container">
    <div class="card">
      <div class="card-header">
        <h1 class="card-title">Comparisons</h1>
        <span style="font-size:13px;color:var(--muted)">{{ .TotalCount }} total</span>
      </div>
      <div style="overflow-x:auto">
        <table class="ci">
          <thead>
            <tr>
              <th>Comparison</th>
              <th>Namespace</th>
              <th>Verdict</th>
              <th>P(imp)</th>
              <th>Tasks</th>
              <th>Baseline Run</th>
              <th>Candidate Run</th>
              <th>Age</th>
            </tr>
          </thead>
          <tbody>
            {{ range .Comparisons }}
            <tr>
              <td><a href="{{ .Link }}" class="mono" style="font-weight:600">{{ .Name }}</a></td>
              <td><span class="mono" style="font-size:13px">{{ .Namespace }}</span></td>
              <td><span class="chip {{ .VerdictChip }}"><span class="dot"></span>{{ .Verdict }}</span></td>
              <td class="num">{{ .PImprovement }}</td>
              <td class="num">{{ .Tasks }}</td>
              <td><a href="/runs/{{ .Namespace }}/{{ .BaselineRun }}" class="mono" style="font-size:13px">{{ .BaselineRun }}</a></td>
              <td><a href="/runs/{{ .Namespace }}/{{ .CandidateRun }}" class="mono" style="font-size:13px">{{ .CandidateRun }}</a></td>
              <td style="color:var(--muted);font-size:13px">{{ .Age }}</td>
            </tr>
            {{ else }}
            <tr>
              <td colspan="8" class="empty-state">No Comparisons found in namespace {{ .Namespace }}.</td>
            </tr>
            {{ end }}
          </tbody>
        </table>
      </div>
      {{ if or .HasPrev .HasMore }}
      <div class="pagination" style="padding:14px 24px;border-top:1px solid var(--line);display:flex;justify-content:space-between;align-items:center;font-size:13px">
        <div>
          {{ if .HasPrev }}<a href="{{ .PrevLink }}">← Previous</a>{{ end }}
        </div>
        <div style="color:var(--muted)">{{ .ShowingRange }}</div>
        <div>
          {{ if .HasMore }}<a href="{{ .NextLink }}">More →</a>{{ end }}
        </div>
      </div>
      {{ end }}
    </div>
  </main>
</body>
</html>
`

const ComparisonTableTemplate = `{{ define "comparison_table" }}
<div style="overflow-x:auto;margin-top:16px">
  <table class="ci">
    <thead>
      <tr>
        <th>eval</th>
        <th>base</th>
        <th>this PR</th>
        <th colspan="2">Δ (95% CI)</th>
        <th>label</th>
        <th>runs</th>
      </tr>
    </thead>
    <tbody>
      {{ range .Tasks }}
      <tr>
        <td class="mono" style="font-weight:600">{{ .ID }}</td>
        <td class="num">{{ .BaseRate }}</td>
        <td class="num">{{ .CandRate }}</td>
        <td class="num">{{ .Delta }}{{ if .HasCI }} <span style="font-size:13px;color:var(--muted)">{{ .CILabel }}</span>{{ end }}</td>
        <td>
          {{ if .HasCI }}
          <div class="cibar" title="Δ: {{ .Delta }} {{ .CILabel }}">
            <div class="axis"></div>
            <div class="zero" style="left:170px"></div>
            <div class="range {{ .StatusClass }}-c" style="left:{{ printf "%.1f" .CILeftPx }}px; width:{{ printf "%.1f" .CIWidthPx }}px"></div>
            <div class="pt {{ .StatusClass }}-c" style="left:{{ printf "%.1f" .CIDotPx }}px"></div>
          </div>
          {{ else }}
          <span style="color:var(--muted)">—</span>
          {{ end }}
        </td>
        <td><span class="chip {{ .StatusClass }}"><span class="dot"></span>{{ .Label }}</span></td>
        <td class="num">
          {{ if gt .SamplesRun 0 }}{{ .SamplesRun }}{{ else }}{{ .Pairs }}{{ end }}
          {{ if or (gt .DroppedCand 0) (gt .DroppedBase 0) }}
          <span style="font-size:11px;color:var(--muted);display:block">dropped: {{ .DroppedCand }} cand, {{ .DroppedBase }} base</span>
          {{ end }}
        </td>
      </tr>
      {{ else }}
      <tr>
        <td colspan="7" class="empty-state">No task comparisons recorded.</td>
      </tr>
      {{ end }}
    </tbody>
  </table>
</div>
{{ end }}`

const comparisonDetailTemplate = `
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ .Name }} · Comparison · eval-runner</title>
  <style>` + baseCSS + `</style>
</head>
<body>
  <header class="header-bar">
    <div class="header-brand">
      <a href="/runs" class="header-logo">eval-runner</a>
      <nav class="nav-links">
        <a href="/runs" class="nav-link {{ if eq .ActiveTab "runs" }}active{{ end }}">Runs</a>
        <a href="/comparisons" class="nav-link {{ if eq .ActiveTab "comparisons" }}active{{ end }}">Comparisons</a>
      </nav>
    </div>
    <div style="font-size:13px;color:var(--muted)">namespace: <span class="mono">{{ .Namespace }}</span></div>
  </header>

  <main class="container">
    <div class="card">
      <div class="card-header">
        <div style="display:flex;align-items:center;gap:12px">
          <h1 class="card-title mono">{{ .Name }}</h1>
        </div>
        <div style="font-size:13px;color:var(--muted)"><a href="/comparisons">← Back to Comparisons</a></div>
      </div>
      <div class="card-body">
        <div class="verdict">
          <span class="chip {{ .VerdictChip }}" style="font-size:16px;padding:4px 14px"><span class="dot"></span>{{ .Verdict }}</span>
          <span class="verdict-headline">{{ .Headline }}</span>
        </div>

        {{ if .Message }}
        <div class="msg-alert"><b>Status:</b> {{ .Message }}</div>
        {{ end }}

        {{ template "comparison_table" . }}
      </div>

      <div class="card-footer">
        <span><b>Base:</b> commit <span class="mono">{{ if .BaseCommitShort }}{{ .BaseCommitShort }}{{ else }}—{{ end }}</span> (<a href="/runs/{{ .Namespace }}/{{ .BaseRun }}">{{ .BaseRun }}</a>)</span>
        <span><b>Candidate:</b> commit <span class="mono">{{ if .CandCommitShort }}{{ .CandCommitShort }}{{ else }}—{{ end }}</span> (<a href="/runs/{{ .Namespace }}/{{ .CandRun }}">{{ .CandRun }}</a>)</span>
        <span><b>Samples:</b> {{ if .SamplesSummary }}{{ .SamplesSummary }}{{ else }}{{ .TotalSamples }} run · {{ .TotalPairs }} paired{{ if or (gt .TotalDroppedCand 0) (gt .TotalDroppedBase 0) }} · dropped: {{ .TotalDroppedCand }} cand, {{ .TotalDroppedBase }} base{{ end }}{{ end }}</span>
        {{ if .SummaryPath }}
        <span style="font-size:12px" class="mono" title="{{ .SummaryPath }}">{{ .SummaryPath }}</span>
        {{ end }}
      </div>
    </div>
  </main>
</body>
</html>
`

var (
	tmplRunsList         = template.Must(template.New("runs_list").Parse(runsListTemplate))
	tmplRunDetail        = template.Must(template.New("run_detail").Parse(runDetailTemplate))
	tmplComparisonsList  = template.Must(template.New("comparisons_list").Parse(comparisonsListTemplate))
	tmplComparisonTable  = template.Must(template.New("comparison_table").Parse(ComparisonTableTemplate))
	tmplComparisonDetail = template.Must(template.Must(template.New("comparison_detail").Parse(ComparisonTableTemplate)).Parse(comparisonDetailTemplate))

	// TmplComparisonTable is the parsed template for the comparison table sub-template.
	TmplComparisonTable = tmplComparisonTable
)

// RenderComparisonTable renders the comparison table sub-template to the given writer.
func RenderComparisonTable(w io.Writer, view ComparisonTableView) error {
	return tmplComparisonTable.ExecuteTemplate(w, "comparison_table", view)
}
