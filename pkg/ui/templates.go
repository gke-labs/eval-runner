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
	"net/url"
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
.wordmark-dot {
  font-size: 0.8em;
  opacity: 0.7;
}
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

/* Live Task Progress */
.progress-container { display: flex; align-items: center; gap: 8px; }
.progress-bar { position: relative; height: 8px; width: 140px; background: var(--line); border-radius: 999px; overflow: hidden; }
.progress-fill { height: 100%; background: var(--teal); border-radius: 999px; }
.progress-text { font-size: 12px; color: var(--muted); font-family: var(--mono); white-space: nowrap; }

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
a.log-path { color: var(--indigo); text-decoration: none; }
a.log-path:hover { text-decoration: underline; background: var(--indigo-soft); }
.msg-alert { margin-top: 14px; padding: 10px 14px; background: var(--bad-soft); color: var(--bad); border-radius: 6px; font-size: 14px; }

/* Metric Filter Bar */
.metric-filter-bar { display: flex; align-items: center; gap: 8px; margin: 18px 0 16px; flex-wrap: wrap; }
.filter-label { font-size: 13px; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; margin-right: 4px; }
.filter-chip { font-size: 13px; font-weight: 500; color: var(--ink-2); background: var(--neutral-soft); padding: 5px 12px; border-radius: 6px; text-decoration: none; border: 1px solid transparent; transition: all 0.15s ease; }
.filter-chip:hover { text-decoration: none; background: var(--line); color: var(--ink); }
.filter-chip.active { background: var(--indigo-soft); color: var(--indigo); font-weight: 600; border-color: rgba(79,70,229,0.2); }

/* Comparisons: timestamps, reason lines, candidate groups, and run deltas */
.section-title { font-size: 15px; font-weight: 600; color: var(--ink-2); margin: 0; padding: 16px 24px 4px; }
.ts { font-family: var(--mono); font-size: 12px; color: var(--muted); white-space: nowrap; }
.reason-line { margin-top: 4px; max-width: 380px; font-size: 12px; line-height: 1.4; color: var(--ink-2); white-space: normal; }
table.ci td.cell-top, table.ci td.cmp-cell { vertical-align: top; }
table.ci td.cmp-cell { min-width: 280px; }
table.ci td.delta-cell .delta-strip { flex-direction: column; gap: 1px; margin: 0; }
.cmp-line { margin-bottom: 4px; }
.cmp-meta { font-size: 12px; color: var(--muted); }
.delta-strip { display: flex; flex-wrap: wrap; gap: 2px 12px; font-size: 12px; margin: 2px 0; }
.delta-item { white-space: nowrap; }
.delta-k { color: var(--muted); }
.delta-v { font-family: var(--mono); font-variant-numeric: tabular-nums; }
.delta-good { color: var(--good); }
.delta-bad { color: var(--bad); }
.delta-zero { color: var(--neutral); }
.delta-missing { color: var(--muted); }
.delta-strip-detail { margin: -8px 0 16px; }
.delta-strip-detail .delta-strip { font-size: 14px; gap: 4px 20px; }
.reason-panel { margin-top: 14px; padding: 10px 14px; background: var(--neutral-soft); border: 1px solid var(--line); border-left: 3px solid var(--neutral); border-radius: 6px; font-size: 14px; color: var(--ink-2); }
.reason-panel .reason-label { font-size: 11px; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 4px; }
.reason-panel .reason-text { white-space: pre-line; line-height: 1.5; }
`

const wordmarkPartial = `{{- define "wordmark" }}<span class="wordmark"><span class="wordmark-dot">.</span>evals</span>{{- end -}}`

const repoHeaderPartial = wordmarkPartial + `{{- define "repo_header" }}
  <header class="header-bar">
    <div class="header-brand">
      {{- if .Repo.Host }}
      <div style="display:flex;align-items:center;gap:8px;font-size:16px">
        <a href="{{ repoIndexLink .LinkNamespace }}" class="header-logo">{{ template "wordmark" }}</a>
        <span style="color:var(--muted)">/</span>
        <span class="mono" style="color:var(--muted)">{{ .Repo.Host }}</span>
        <span style="color:var(--muted)">/</span>
        <span class="mono" style="color:var(--muted)">{{ .Repo.Owner }}</span>
        <span style="color:var(--muted)">/</span>
        <a href="{{ repoLink .Repo "" .LinkNamespace }}" class="mono" style="font-weight:600;color:var(--ink)">{{ .Repo.Name }}</a>
      </div>
      <nav class="nav-links">
        <a href="{{ repoLink .Repo "" .LinkNamespace }}" class="nav-link {{ if eq .ActiveTab "overview" }}active{{ end }}">Overview</a>
        <a href="{{ repoLink .Repo "#suites" .LinkNamespace }}" class="nav-link {{ if eq .ActiveTab "evals" }}active{{ end }}">Evals</a>
        <a href="{{ repoLink .Repo "/runs" .LinkNamespace }}" class="nav-link {{ if eq .ActiveTab "runs" }}active{{ end }}">Runs</a>
        <a href="{{ repoLink .Repo "/pulls" .LinkNamespace }}" class="nav-link {{ if eq .ActiveTab "pulls" }}active{{ end }}">Pull requests</a>
        <a href="{{ repoLink .Repo "/comparisons" .LinkNamespace }}" class="nav-link {{ if eq .ActiveTab "comparisons" }}active{{ end }}">Comparisons</a>
        <a href="{{ repoLink .Repo "/series" .LinkNamespace }}" class="nav-link {{ if eq .ActiveTab "series" }}active{{ end }}">Series</a>
      </nav>
      {{- else }}
      <a href="/runs" class="header-logo">{{ template "wordmark" }}</a>
      <nav class="nav-links">
        <a href="/pulls" class="nav-link {{ if eq .ActiveTab "pulls" }}active{{ end }}">Pull Requests</a>
        <a href="/runs" class="nav-link {{ if eq .ActiveTab "runs" }}active{{ end }}">Runs</a>
        <a href="/comparisons" class="nav-link {{ if eq .ActiveTab "comparisons" }}active{{ end }}">Comparisons</a>
      </nav>
      {{- end }}
    </div>
    {{- if .LinkNamespace }}
    <span class="chip neutral mono" style="font-size:12px">namespace: {{ .LinkNamespace }}</span>
    {{- end }}
  </header>{{- end -}}`

const repoIndexTemplate = `
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Repositories · eval-runner</title>
  <style>` + baseCSS + `</style>
</head>
<body>
  <header class="header-bar">
    <div class="header-brand">
      <a href="{{ repoIndexLink .LinkNamespace }}" class="header-logo">{{ template "wordmark" }}</a>
    </div>
    {{- if .LinkNamespace }}
    <span class="badge chip neutral mono" style="font-size:12px">namespace: {{ .LinkNamespace }}</span>
    {{- end }}
  </header>

  <main class="container">
    <div class="card">
      <div class="card-header">
        <h1 class="card-title">Repositories</h1>
        <span style="font-size:13px;color:var(--muted)">{{ .TotalCount }} total</span>
      </div>
      <div style="overflow-x:auto">
        <table class="ci">
          <thead>
            <tr>
              <th>Repository</th>
              <th>Runs</th>
              <th>Comparisons</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            {{ range .Repositories }}
            <tr>
              <td><a href="{{ .Link }}" class="mono" style="font-weight:600">{{ .Repository }}</a></td>
              <td class="num">{{ .RunsCount }}</td>
              <td class="num">{{ .ComparisonsCount }}</td>
              <td><a href="{{ .Link }}" class="mono" style="font-size:13px">view repository →</a></td>
            </tr>
            {{ else }}
            <tr>
              <td colspan="4" class="empty-state">No repositories found.</td>
            </tr>
            {{ end }}
          </tbody>
        </table>
      </div>
    </div>
  </main>
</body>
</html>
`

const repoOverviewTemplate = `
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ if .Repo.Name }}{{ .Repo.Name }} · {{ end }}Overview · eval-runner</title>
  <style>` + baseCSS + `
.overview-headline {
  display: flex;
  align-items: baseline;
  flex-wrap: wrap;
  gap: 12px 24px;
  margin-bottom: 24px;
  padding: 16px 20px;
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: 8px;
}
.overview-headline .headline-score {
  font-size: 28px;
  font-weight: 700;
  color: var(--ink);
  font-variant-numeric: tabular-nums;
}
.overview-headline .headline-ci {
  font-size: 16px;
  color: var(--muted);
  font-variant-numeric: tabular-nums;
  margin-left: 6px;
}
.overview-headline .headline-meta {
  font-size: 14px;
  color: var(--muted);
  margin-left: auto;
}
.suite-row {
  background: var(--neutral-soft);
  font-weight: 600;
}
.suite-row td {
  border-top: 1px solid var(--line);
  border-bottom: 1px solid var(--line);
}
.eval-row td.eval-name {
  padding-left: 32px;
}
.sparkline-svg {
  display: block;
}
</style>
</head>
<body>
  {{ template "repo_header" . }}

  <main class="container">
    {{- if .IsEmpty }}
    <div class="card">
      <div class="card-header">
        <h1 class="card-title">Overview</h1>
        <span style="font-size:13px;color:var(--muted)">branch: <span class="mono">{{ .Branch }}</span></span>
      </div>
      <div class="card-body">
        <div class="empty-state">
          <p>No observations recorded on <code class="mono">{{ .Branch }}</code>. Produce an observation by applying a baseline Run: <code class="mono">kubectl apply -f &lt;run.yaml&gt;</code></p>
        </div>
      </div>
    </div>
    {{- else }}
    <!-- 1. Headline -->
    <div class="overview-headline">
      <div>
        <span class="headline-score">{{ .OverallScore }}</span>
        <span class="headline-ci">{{ .OverallCILabel }}</span>
      </div>
      <div class="headline-meta">
        at commit {{ if .OverallCommitLink }}<a href="{{ .OverallCommitLink }}" class="mono">{{ .OverallCommitShort }}</a>{{ else }}<span class="mono">{{ .OverallCommitShort }}</span>{{ end }} · <span title="{{ .OverallTimeUTC }}">{{ .OverallTimeStr }}</span>
      </div>
    </div>

    <!-- 2 & 3. Eval Suites & Evals Table -->
    <div class="card" id="suites">
      <div class="card-header">
        <h1 class="card-title">Eval Suites</h1>
        <span style="font-size:13px;color:var(--muted)">branch: <span class="mono">{{ .Branch }}</span></span>
      </div>
      <div style="overflow-x:auto">
        <table class="ci">
          <thead>
            <tr>
              <th>Suite / Eval</th>
              <th>Score (95% CI)</th>
              <th>Evals</th>
              <th>Δ vs prev</th>
              <th>Trend</th>
            </tr>
          </thead>
          <tbody>
            {{- range .Suites }}
            <tr class="suite-row">
              <td>
                {{- if .SuiteLink }}
                <a href="{{ .SuiteLink }}" class="mono" style="font-weight:600">{{ .Name }}</a>
                {{- else }}
                <span class="mono" style="font-weight:600">{{ .Name }}</span>
                {{- end }}
              </td>
              <td>
                <span class="mono" style="font-weight:600">{{ .ScoreStr }}</span>
                <span style="font-size:12px;color:var(--muted);margin-left:4px">{{ .CILabel }}</span>
              </td>
              <td class="num">{{ .EvalsCount }}</td>
              <td>
                {{- if .HasDelta }}
                <span class="chip {{ .DeltaClass }}">{{ .DeltaStr }}</span>
                {{- else }}
                <span style="color:var(--muted)">—</span>
                {{- end }}
              </td>
              <td>
                {{- if .Sparkline.HasPoints }}
                <svg viewBox="{{ .Sparkline.ViewBox }}" width="120" height="28" class="sparkline-svg">
                  {{- range .Sparkline.Polygons }}
                  <polygon points="{{ . }}" fill="var(--indigo)" fill-opacity="0.15" stroke="none"/>
                  {{- end }}
                  {{- range .Sparkline.Polylines }}
                  <polyline points="{{ .Points }}" fill="none" stroke="{{ .Stroke }}" stroke-width="1.5"/>
                  {{- end }}
                  {{- range .Sparkline.Circles }}
                  <circle cx="{{ .CX }}" cy="{{ .CY }}" r="{{ .R }}" fill="var(--indigo)"/>
                  {{- end }}
                  {{- range .Sparkline.Ticks }}
                  <line x1="{{ .X1 }}" y1="{{ .Y1 }}" x2="{{ .X2 }}" y2="{{ .Y2 }}" stroke="{{ if .Stroke }}{{ .Stroke }}{{ else }}var(--indigo){{ end }}" stroke-width="1.5" stroke-linecap="round" class="obs-tick"/>
                  {{- end }}
                </svg>
                {{- else }}
                <span style="color:var(--muted)">—</span>
                {{- end }}
              </td>
            </tr>
            {{- range .Evals }}
            <tr class="eval-row">
              <td class="eval-name mono">
                {{- if .SamplesLink }}
                <a href="{{ .SamplesLink }}">{{ .ID }}</a>
                {{- else }}
                {{ .ID }}
                {{- end }}
              </td>
              <td>
                {{- if .RunLink }}
                <a href="{{ .RunLink }}" class="mono">{{ .ScoreStr }}</a>
                {{- else }}
                <span class="mono">{{ .ScoreStr }}</span>
                {{- end }}
                <span style="font-size:12px;color:var(--muted);margin-left:4px">{{ .CILabel }}</span>
              </td>
              <td class="num" style="color:var(--muted)">—</td>
              <td>
                {{- if .HasDelta }}
                <span class="chip {{ .DeltaClass }}">{{ .DeltaStr }}</span>
                {{- else }}
                <span style="color:var(--muted)">—</span>
                {{- end }}
              </td>
              <td>
                {{- if .Sparkline.HasPoints }}
                <svg viewBox="{{ .Sparkline.ViewBox }}" width="120" height="28" class="sparkline-svg">
                  {{- range .Sparkline.Polygons }}
                  <polygon points="{{ . }}" fill="var(--indigo)" fill-opacity="0.15" stroke="none"/>
                  {{- end }}
                  {{- range .Sparkline.Polylines }}
                  <polyline points="{{ .Points }}" fill="none" stroke="{{ .Stroke }}" stroke-width="1.5"/>
                  {{- end }}
                  {{- range .Sparkline.Circles }}
                  <circle cx="{{ .CX }}" cy="{{ .CY }}" r="{{ .R }}" fill="var(--indigo)"/>
                  {{- end }}
                  {{- range .Sparkline.Ticks }}
                  <line x1="{{ .X1 }}" y1="{{ .Y1 }}" x2="{{ .X2 }}" y2="{{ .Y2 }}" stroke="{{ if .Stroke }}{{ .Stroke }}{{ else }}var(--indigo){{ end }}" stroke-width="1.5" stroke-linecap="round" class="obs-tick"/>
                  {{- end }}
                </svg>
                {{- else }}
                <span style="color:var(--muted)">—</span>
                {{- end }}
              </td>
            </tr>
            {{- end }}
            {{- end }}
          </tbody>
        </table>
      </div>
      <div style="padding:14px 24px;border-top:1px solid var(--line);font-size:12px;color:var(--muted);line-height:1.5">
        Score at main for an eval is the pooled success rate (Wilson score interval) over the newest completed baseline observations on <code class="mono">{{ .Branch }}</code>. Suite scores are the mean of their evals' scores with the confidence interval from pooled samples. Overall score is the mean over suites.
      </div>
    </div>

    <!-- 4. Tiles below the table -->
    <div class="meta-grid" style="margin-top:24px">
      <div class="meta-tile">
        <div class="label">Runs</div>
        <div class="value num"><a href="{{ .RunsLink }}">{{ .TotalRunsCount }}</a></div>
      </div>
      <div class="meta-tile">
        <div class="label">Comparisons</div>
        <div class="value num"><a href="{{ .ComparisonsLink }}">{{ .TotalComparisonsCount }}</a></div>
      </div>
      <div class="meta-tile">
        <div class="label">Pull Requests</div>
        <div class="value num"><a href="{{ .PullsLink }}">{{ .TotalPullsCount }}</a></div>
      </div>
    </div>
    {{- end }}
  </main>
</body>
</html>
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
<body>{{ template "repo_header" . }}

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
              <td><span class="chip {{ .PhaseChip }}"><span class="dot"></span>{{ .Phase }}</span></td>
              <td class="num">{{ .Tasks }}</td>
              <td class="num">{{ if .SampleRange }}{{ .SampleRange }}{{ else }}{{ .Samples }}{{ end }}</td>
              <td class="mono" style="font-size:13px">{{ if .Rates }}{{ .Rates }}{{ else }}—{{ end }}</td>
              <td><span class="mono" style="font-size:13px">{{ if .Commit }}{{ .Commit }}{{ else }}—{{ end }}</span></td>
              <td style="color:var(--muted);font-size:13px">{{ .Age }}</td>
            </tr>
            {{ else }}
            <tr>
              <td colspan="7" class="empty-state">No Runs found.</td>
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
  {{- if eq .Phase "Running" }}
  <meta http-equiv="refresh" content="15">
  {{- end }}
  <style>` + baseCSS + `</style>
</head>
<body>{{ template "repo_header" . }}

  <main class="container">
    <div class="card">
      <div class="card-header">
        <h1 class="card-title">
          <span class="mono">{{ .Name }}</span>
          <span class="chip {{ .PhaseChip }}"><span class="dot"></span>{{ .Phase }}</span>
        </h1>
        <div style="font-size:13px;color:var(--muted)"><a href="{{ if .Repo.Host }}{{ repoLink .Repo "/runs" .LinkNamespace }}{{ else }}/runs{{ end }}">← Back to Runs</a></div>
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
            <div class="value mono" style="font-size:12px">{{ if .ArtifactsPrefix }}{{ if .ArtifactsURL }}<a class="log-path mono" href="{{ .ArtifactsURL }}" target="_blank" rel="noopener noreferrer" title="{{ .ArtifactsPrefix }}">{{ .ArtifactsPrefix }}</a>{{ else }}<span class="log-path" title="{{ .ArtifactsPrefix }}">{{ .ArtifactsPrefix }}</span>{{ end }}{{ else }}—{{ end }}</div>
          </div>
        </div>

        {{ if or .TotalCost .TokensSummary .LatencySummary }}
        <div class="meta-grid">
          <div class="meta-tile">
            <div class="label">Total Cost</div>
            <div class="value num">{{ if .TotalCost }}{{ .TotalCost }}{{ else }}—{{ end }}</div>
          </div>
          <div class="meta-tile">
            <div class="label">Tokens Consumed</div>
            <div class="value num">{{ if .TokensSummary }}{{ .TokensSummary }}{{ else }}—{{ end }}</div>
          </div>
          <div class="meta-tile">
            <div class="label">Latency (mean of per-task p50 / p95)</div>
            <div class="value num">{{ if .LatencySummary }}{{ .LatencySummary }}{{ else }}—{{ end }}</div>
          </div>
        </div>
        {{ end }}

        {{ if .Message }}
        <div class="msg-alert"><b>Message:</b> {{ .Message }}</div>
        {{ end }}

        <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;margin:24px 0 12px;gap:12px">
          <h2 style="font-size:17px;font-weight:600;margin:0">Eval Tasks</h2>
          <div class="metric-filter-bar" style="margin:0">
            <span class="filter-label">View:</span>
            <a href="{{ metricLink .LinkNamespace "all" }}" class="filter-chip {{ if or (eq .Metric "all") (eq .Metric "") }}active{{ end }}">All Metrics</a>
            <a href="{{ metricLink .LinkNamespace "rate" }}" class="filter-chip {{ if eq .Metric "rate" }}active{{ end }}">Accuracy / Rate</a>
            <a href="{{ metricLink .LinkNamespace "latency" }}" class="filter-chip {{ if eq .Metric "latency" }}active{{ end }}">Latency (p50/p95)</a>
            <a href="{{ metricLink .LinkNamespace "tokens" }}" class="filter-chip {{ if eq .Metric "tokens" }}active{{ end }}">Tokens</a>
            <a href="{{ metricLink .LinkNamespace "cost" }}" class="filter-chip {{ if eq .Metric "cost" }}active{{ end }}">Cost ($ USD)</a>
          </div>
        </div>
        <div style="overflow-x:auto">
          <table class="ci">
            <thead>
              <tr>
                <th>eval</th>
                <th>phase</th>
                {{ if or (eq .Metric "all") (eq .Metric "") (eq .Metric "rate") }}
                <th>rate</th>
                <th colspan="2">95% CI (0-1 axis)</th>
                {{ end }}
                {{ if or (eq .Metric "all") (eq .Metric "") (eq .Metric "latency") }}
                <th>latency (p50/p95)</th>
                {{ end }}
                {{ if or (eq .Metric "all") (eq .Metric "") (eq .Metric "tokens") }}
                <th>tokens</th>
                {{ if eq .Metric "tokens" }}
                <th>input</th>
                <th>output</th>
                <th>reasoning</th>
                <th>cached</th>
                {{ end }}
                {{ end }}
                {{ if or (eq .Metric "all") (eq .Metric "") (eq .Metric "cost") }}
                <th>cost</th>
                {{ end }}
                <th>n</th>
                {{ if or (eq .Metric "all") (eq .Metric "") (eq .Metric "rate") }}
                <th>method</th>
                {{ end }}
                <th>log path</th>
              </tr>
            </thead>
            <tbody>
              {{ range .Tasks }}
              <tr>
                <td class="mono" style="font-weight:600">
                  {{- if .SamplesLink }}
                  <a href="{{ .SamplesLink }}">{{ .ID }}</a>
                  {{- else }}
                  {{- .ID }}
                  {{- if .ArtifactsUnavailable }} <span style="font-size:11px;font-weight:normal;color:var(--muted)">(artifacts unavailable)</span>{{ end }}
                  {{- end }}
                </td>
                <td><span class="chip {{ .PhaseChip }}"><span class="dot"></span>{{ .Phase }}</span></td>
                {{ if or (eq $.Metric "all") (eq $.Metric "") (eq $.Metric "rate") }}
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
                  {{ else if eq .Phase "Running" }}
                  <div class="progress-container">
                    <div class="progress-bar" title="{{ .ProgressText }}">
                      <div class="progress-fill" style="width:{{ printf "%.1f" .ProgressPct }}%"></div>
                    </div>
                    <span class="progress-text">{{ .ProgressText }}</span>
                  </div>
                  {{ else }}
                  <span style="color:var(--muted)">—</span>
                  {{ end }}
                </td>
                {{ end }}
                {{ if or (eq $.Metric "all") (eq $.Metric "") (eq $.Metric "latency") }}
                <td class="num mono">{{ .LatencyStr }}</td>
                {{ end }}
                {{ if or (eq $.Metric "all") (eq $.Metric "") (eq $.Metric "tokens") }}
                <td class="num mono">{{ .TokensStr }}</td>
                {{ if eq $.Metric "tokens" }}
                <td class="num mono">{{ .InputTokens }}</td>
                <td class="num mono">{{ .OutputTokens }}</td>
                <td class="num mono">{{ .ReasoningStr }}</td>
                <td class="num mono">{{ .CacheReadStr }}</td>
                {{ end }}
                {{ end }}
                {{ if or (eq $.Metric "all") (eq $.Metric "") (eq $.Metric "cost") }}
                <td class="num mono"{{ if .CostTooltip }} title="{{ .CostTooltip }}"{{ end }}>{{ .CostStr }}</td>
                {{ end }}
                <td class="num">{{ if gt .Samples 0 }}{{ .Successes }}/{{ .Samples }}{{ if .SampleRange }} <span style="font-size:11px;color:var(--muted)">({{ .SampleRange }})</span>{{ end }}{{ else }}—{{ end }}</td>
                {{ if or (eq $.Metric "all") (eq $.Metric "") (eq $.Metric "rate") }}
                <td class="mono" style="font-size:13px">{{ if .Method }}{{ .Method }}{{ else }}—{{ end }}</td>
                {{ end }}
                <td>
                  {{ if .LogPath }}
                  {{ if .SamplesLink }}
                  <a href="{{ .SamplesLink }}" class="log-path mono" title="{{ .LogPath }}">{{ .LogPath }}</a>
                  {{ else }}
                  <span class="log-path mono" title="{{ .LogPath }}">{{ .LogPath }}</span>
                  {{ end }}
                  {{ if .LogURL }}<a href="{{ .LogURL }}" target="_blank" rel="noopener noreferrer" title="open in Cloud Storage" style="margin-left:4px">&#8599;</a>{{ end }}
                  {{ else }}
                  <span style="color:var(--muted)">—</span>
                  {{ end }}
                </td>
              </tr>
              {{ if .Message }}
              <tr>
                <td colspan="12" style="padding-top:0;padding-bottom:12px;color:var(--muted);font-size:12px">
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
      <div class="card-footer">
        <span>{{ .ArtifactFooter }}</span>
        {{- if .Version }}
        <span>eval-runner {{ .Version }}</span>
        {{- end }}
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
<body>{{ template "repo_header" . }}

  <main class="container">
    <div class="card">
      <div class="card-header">
        <h1 class="card-title">Comparisons</h1>
        <div style="display:flex;align-items:center;gap:16px;flex-wrap:wrap">
          {{- if .ShowKindFilter }}
          <div class="metric-filter-bar" style="margin:0">
            <span class="filter-label">Show:</span>
            <a href="{{ .KindAllLink }}" class="filter-chip {{ if eq .Kind "" }}active{{ end }}">All</a>
            <a href="{{ .KindBaselineLink }}" class="filter-chip {{ if eq .Kind "baseline" }}active{{ end }}">vs baseline</a>
            <a href="{{ .KindPreviousLink }}" class="filter-chip {{ if eq .Kind "previous" }}active{{ end }}">vs previous</a>
          </div>
          {{- end }}
          <span style="font-size:13px;color:var(--muted)">{{ .TotalCount }} total</span>
        </div>
      </div>
      {{- if .ShowGroups }}
      {{- if .ShowFlat }}
      <h2 class="section-title">By candidate run</h2>
      {{- end }}
      <div style="overflow-x:auto">
        <table class="ci">
          <thead>
            <tr>
              <th>Candidate Run</th>
              <th>Candidate Created</th>
              {{- if .ShowBaselineColumn }}
              <th>vs baseline</th>
              {{- end }}
              {{- if .ShowPreviousColumn }}
              <th>vs previous</th>
              {{- end }}
            </tr>
          </thead>
          <tbody>
            {{- range .Groups }}
            <tr>
              <td class="cell-top"><a href="{{ .CandidateLink }}" class="mono" style="font-weight:600">{{ .CandidateRun }}</a></td>
              <td class="cell-top ts">{{ .Created }}</td>
              {{- if $.ShowBaselineColumn }}
              <td class="cmp-cell">{{ template "comparison_cell" .VsBaseline }}</td>
              {{- end }}
              {{- if $.ShowPreviousColumn }}
              <td class="cmp-cell">{{ template "comparison_cell" .VsPrevious }}</td>
              {{- end }}
            </tr>
            {{- else }}
            <tr>
              <td colspan="4" class="empty-state">No Comparisons with label {{ $.KindLabelKey }}=true.</td>
            </tr>
            {{- end }}
          </tbody>
        </table>
      </div>
      {{- end }}
      {{- if .ShowFlat }}
      {{- if .ShowGroups }}
      <h2 class="section-title">Other comparisons</h2>
      {{- end }}
      <div style="overflow-x:auto">
        <table class="ci">
          <thead>
            <tr>
              <th>Comparison</th>
              <th>Verdict</th>
              <th>P(imp)</th>
              <th>Run deltas</th>
              <th>Tasks</th>
              <th>Baseline Run</th>
              <th>Candidate Run</th>
              <th>Completed</th>
              <th>Age</th>
            </tr>
          </thead>
          <tbody>
            {{ range .Comparisons }}
            <tr>
              <td><a href="{{ .Link }}" class="mono" style="font-weight:600">{{ .Name }}</a></td>
              <td><span class="chip {{ .VerdictChip }}"><span class="dot"></span>{{ .Verdict }}</span>{{ if .Reason }}<div class="reason-line" title="{{ .ReasonFull }}">{{ .Reason }}</div>{{ end }}</td>
              <td class="num">{{ .PImprovement }}</td>
              <td class="delta-cell">{{ template "delta_strip" .Deltas }}</td>
              <td class="num">{{ .Tasks }}</td>
              <td><a href="{{ runLink $.Repo .BaselineRun $.LinkNamespace .Namespace }}" class="mono" style="font-size:13px">{{ .BaselineRun }}</a></td>
              <td><a href="{{ runLink $.Repo .CandidateRun $.LinkNamespace .Namespace }}" class="mono" style="font-size:13px">{{ .CandidateRun }}</a></td>
              <td class="ts">{{ .Completed }}</td>
              <td style="color:var(--muted);font-size:13px">{{ .Age }}</td>
            </tr>
            {{ else }}
            <tr>
              <td colspan="9" class="empty-state">No Comparisons found.</td>
            </tr>
            {{ end }}
          </tbody>
        </table>
      </div>
      {{- end }}
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

// reasonCSS styles the structured Reason panel on the Comparison detail page.
const reasonCSS = `
.reason-panel .reason-headline { font-size: 15px; font-weight: 600; color: var(--ink); line-height: 1.4; }
.reason-panel .reason-block { margin-top: 10px; }
.reason-panel .reason-block-heading { font-size: 12px; font-weight: 600; color: var(--ink-2); margin: 8px 0 2px; }
.reason-panel .reason-para { margin: 0; line-height: 1.5; }
.reason-panel .reason-list { margin: 2px 0 0; padding-left: 20px; line-height: 1.5; }
.reason-panel a { overflow-wrap: anywhere; }
`

// reasonSegmentsTemplate renders a []ReasonSegment. Link segments become anchors that open in
// a new tab; html/template escapes both the text and the URL.
const reasonSegmentsTemplate = `{{ define "reason_segments" }}{{ range . }}{{ if .URL }}<a href="{{ .URL }}" target="_blank" rel="noopener noreferrer">{{ .Text }}</a>{{ else }}{{ .Text }}{{ end }}{{ end }}{{ end }}`

// deltaStripTemplate renders the Quality, Tokens and Cost changes of a RunDeltas value.
// Colors encode direction only; the verdict chip carries statistical significance.
const deltaStripTemplate = `{{ define "delta_strip" }}<div class="delta-strip">
<span class="delta-item"><span class="delta-k">Quality</span> {{ template "delta_value" .Quality }}</span>
<span class="delta-item"><span class="delta-k">Tokens</span> {{ template "delta_value" .Tokens }}</span>
<span class="delta-item"><span class="delta-k">Cost</span> {{ template "delta_value" .Cost }}</span>
</div>{{ end }}
{{- define "delta_value" }}{{ if .Text }}<span class="delta-v {{ .Class }}"{{ if .Title }} title="{{ .Title }}"{{ end }}>{{ .Text }}</span>{{ else }}<span class="delta-v delta-missing">—</span>{{ end }}{{ end }}`

// comparisonCellTemplate renders one ComparisonCell of the grouped /comparisons view, or an
// em dash when the candidate Run has no Comparison of that kind.
const comparisonCellTemplate = `{{ define "comparison_cell" }}
{{- if . }}
<div class="cmp-line"><span class="chip {{ .VerdictChip }}"><span class="dot"></span>{{ .Verdict }}</span> <span class="num cmp-meta">P(imp) {{ .PImprovement }}</span></div>
<div class="cmp-line"><a href="{{ .Link }}" class="mono" style="font-size:13px">{{ .Name }}</a>{{ if .BaselineRun }} <span class="cmp-meta">against <a href="{{ .BaselineLink }}" class="mono">{{ .BaselineRun }}</a></span>{{ end }}</div>
<div class="cmp-line cmp-meta">Completed <span class="ts">{{ .Completed }}</span></div>
{{ template "delta_strip" .Deltas }}
{{- if .Reason }}
<div class="reason-line" title="{{ .ReasonFull }}">{{ .Reason }}</div>
{{- end }}
{{- if .Older }}
<div class="cmp-line cmp-meta" style="margin-top:4px">Older: {{ range $i, $o := .Older }}{{ if $i }}, {{ end }}<a href="{{ $o.Link }}" class="mono">{{ $o.Name }}</a>{{ end }}</div>
{{- end }}
{{- else }}
<span style="color:var(--muted)">—</span>
{{- end }}
{{ end }}`

const ComparisonTableTemplate = `{{ define "comparison_table" }}
<div style="overflow-x:auto;margin-top:16px">
  <table class="ci">
    <thead>
      {{ if eq .Metric "rate" }}
      <tr>
        <th>eval</th>{{ if .HasAffinity }}
        <th>affinity</th>{{ end }}
        <th>base</th>
        <th>this PR</th>
        <th colspan="2">Δ (95% CI)</th>
        <th>label</th>
        <th>samples</th>
      </tr>
      {{ else if eq .Metric "latency" }}
      <tr>
        <th>eval</th>{{ if .HasAffinity }}
        <th>affinity</th>{{ end }}
        <th>base latency (p50/p95)</th>
        <th>latency (p50/p95)</th>
        <th>Δ latency (p50)</th>
        <th>label</th>
        <th>samples</th>
      </tr>
      {{ else if eq .Metric "tokens" }}
      <tr>
        <th>eval</th>{{ if .HasAffinity }}
        <th>affinity</th>{{ end }}
        <th>base tokens</th>
        <th>tokens</th>
        <th>Δ tokens</th>
        <th>reasoning (+R)</th>
        <th>samples</th>
      </tr>
      {{ else if eq .Metric "cost" }}
      <tr>
        <th>eval</th>{{ if .HasAffinity }}
        <th>affinity</th>{{ end }}
        <th>base cost ($ USD)</th>
        <th>cost</th>
        <th>Δ cost ($ USD)</th>
        <th>this PR tokens</th>
        <th>samples</th>
      </tr>
      {{ else }}
      <tr>
        <th>eval</th>{{ if .HasAffinity }}
        <th>affinity</th>{{ end }}
        <th>base</th>
        <th>this PR</th>
        <th colspan="2">Δ (95% CI)</th>
        <th>label</th>
        <th>samples</th>
        {{ if or (eq .Metric "tokens") (and (or (eq .Metric "all") (eq .Metric "")) .HasTokens) }}
        <th>tokens</th>
        {{ end }}
        {{ if or (eq .Metric "latency") (and (or (eq .Metric "all") (eq .Metric "")) .HasLatency) }}
        <th>latency (p50/p95)</th>
        {{ end }}
        {{ if or (eq .Metric "cost") (and (or (eq .Metric "all") (eq .Metric "")) .HasCost) }}
        <th>cost</th>
        {{ end }}
      </tr>
      {{ end }}
    </thead>
    <tbody>
      {{ range .Tasks }}
      <tr>
        <td class="mono" style="font-weight:600">{{ .ID }}</td>{{ if $.HasAffinity }}
        <td class="mono">{{ .Affinity }}</td>{{ end }}
        {{ if eq $.Metric "rate" }}
        <td class="num">{{ if .BaseSamplesLink }}<a href="{{ .BaseSamplesLink }}">{{ .BaseRate }}</a>{{ else }}{{ .BaseRate }}{{ end }}</td>
        <td class="num">{{ if .CandSamplesLink }}<a href="{{ .CandSamplesLink }}">{{ .CandRate }}</a>{{ else }}{{ .CandRate }}{{ end }}</td>
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
        <td class="num">{{ if gt .SamplesRun 0 }}{{ .SamplesRun }}{{ else if gt .Pairs 0 }}{{ .Pairs }}{{ else }}—{{ end }}</td>
        {{ else if eq $.Metric "latency" }}
        <td class="num mono">{{ if .BaseLatencyStr }}{{ .BaseLatencyStr }}{{ else }}—{{ end }}</td>
        <td class="num mono">{{ if .CandLatencyStr }}{{ .CandLatencyStr }}{{ else if .LatencyStr }}{{ .LatencyStr }}{{ else }}—{{ end }}</td>
        <td class="num mono">{{ if .DeltaLatencyStr }}{{ .DeltaLatencyStr }}{{ else }}—{{ end }}</td>
        <td><span class="chip {{ .StatusClass }}"><span class="dot"></span>{{ .Label }}</span></td>
        <td class="num">{{ if gt .SamplesRun 0 }}{{ .SamplesRun }}{{ else if gt .Pairs 0 }}{{ .Pairs }}{{ else }}—{{ end }}</td>
        {{ else if eq $.Metric "tokens" }}
        <td class="num mono">{{ if .BaseTokensStr }}{{ .BaseTokensStr }}{{ else }}—{{ end }}</td>
        <td class="num mono">{{ if .CandTokensStr }}{{ .CandTokensStr }}{{ else if .Tokens }}{{ .Tokens.Total }}{{ else }}—{{ end }}</td>
        <td class="num mono">{{ if .DeltaTokensStr }}{{ .DeltaTokensStr }}{{ else }}—{{ end }}</td>
        <td class="num mono">{{ if and .Tokens (gt .Tokens.Reasoning 0) }}+R {{ .Tokens.Reasoning }}{{ else }}—{{ end }}</td>
        <td class="num">{{ if gt .SamplesRun 0 }}{{ .SamplesRun }}{{ else if gt .Pairs 0 }}{{ .Pairs }}{{ else }}—{{ end }}</td>
        {{ else if eq $.Metric "cost" }}
        <td class="num mono"{{ if .BaseCostTooltip }} title="{{ .BaseCostTooltip }}"{{ end }}>{{ if .BaseCostStr }}{{ .BaseCostStr }}{{ else }}—{{ end }}</td>
        <td class="num mono"{{ if or .CandCostTooltip .CostTooltip }} title="{{ if .CandCostTooltip }}{{ .CandCostTooltip }}{{ else }}{{ .CostTooltip }}{{ end }}"{{ end }}>{{ if .CandCostStr }}{{ .CandCostStr }}{{ else if .CostStr }}{{ .CostStr }}{{ else }}—{{ end }}</td>
        <td class="num mono">{{ if .DeltaCostStr }}{{ .DeltaCostStr }}{{ else }}—{{ end }}</td>
        <td class="num mono">{{ if .CandTokensStr }}{{ .CandTokensStr }}{{ else if .Tokens }}{{ .Tokens.Total }}{{ else }}—{{ end }}</td>
        <td class="num">{{ if gt .SamplesRun 0 }}{{ .SamplesRun }}{{ else if gt .Pairs 0 }}{{ .Pairs }}{{ else }}—{{ end }}</td>
        {{ else }}
        <td class="num">{{ if .BaseSamplesLink }}<a href="{{ .BaseSamplesLink }}">{{ .BaseRate }}</a>{{ else }}{{ .BaseRate }}{{ end }}</td>
        <td class="num">{{ if .CandSamplesLink }}<a href="{{ .CandSamplesLink }}">{{ .CandRate }}</a>{{ else }}{{ .CandRate }}{{ end }}</td>
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
          {{ if gt .SamplesRun 0 }}{{ .SamplesRun }}{{ else if gt .Pairs 0 }}{{ .Pairs }}{{ else }}—{{ end }}
          {{ if or (gt .DroppedCand 0) (gt .DroppedBase 0) }}
          <span style="font-size:11px;color:var(--muted);display:block">dropped: {{ .DroppedCand }} cand, {{ .DroppedBase }} base</span>
          {{ end }}
        </td>
        {{ if or (eq $.Metric "tokens") (and (or (eq $.Metric "all") (eq $.Metric "")) $.HasTokens) }}
        <td class="num">{{ if .Tokens }}{{ .Tokens.Total }}{{ if gt .Tokens.Reasoning 0 }} (+R {{ .Tokens.Reasoning }}){{ end }}{{ else if .CandTokensStr }}{{ .CandTokensStr }}{{ else }}—{{ end }}</td>
        {{ end }}
        {{ if or (eq $.Metric "latency") (and (or (eq $.Metric "all") (eq $.Metric "")) $.HasLatency) }}
        <td class="num" style="font-size:13px">{{ if .CandLatencyStr }}{{ .CandLatencyStr }}{{ else if .LatencyStr }}{{ .LatencyStr }}{{ else }}—{{ end }}</td>
        {{ end }}
        {{ if or (eq $.Metric "cost") (and (or (eq $.Metric "all") (eq $.Metric "")) $.HasCost) }}
        <td class="num" style="font-size:13px"{{ if or .CandCostTooltip .CostTooltip }} title="{{ if .CandCostTooltip }}{{ .CandCostTooltip }}{{ else }}{{ .CostTooltip }}{{ end }}"{{ end }}>{{ if .CandCostStr }}{{ .CandCostStr }}{{ else if .CostStr }}{{ .CostStr }}{{ else }}—{{ end }}</td>
        {{ end }}
        {{ end }}
      </tr>
      {{ else }}
      <tr>
        <td colspan="12" class="empty-state">No task comparisons recorded.</td>
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
  <style>` + baseCSS + reasonCSS + `</style>
</head>
<body>{{ template "repo_header" . }}

  <main class="container">
    <div class="card">
      <div class="card-header">
        <div style="display:flex;align-items:center;gap:12px">
          <h1 class="card-title mono">{{ .Name }}</h1>
        </div>
        <div style="font-size:13px;color:var(--muted)"><a href="{{ if .Repo.Host }}{{ repoLink .Repo "/comparisons" .LinkNamespace }}{{ else }}/comparisons{{ end }}">← Back to Comparisons</a></div>
      </div>
      <div class="card-body">
        <div class="verdict">
          <span class="chip {{ .VerdictChip }}" style="font-size:16px;padding:4px 14px"><span class="dot"></span>{{ .Verdict }}</span>
          <span class="verdict-headline">{{ .Headline }}</span>{{ if .HasAffinity }}
          <span class="chip neutral" style="font-size:13px;padding:2px 8px">{{ .AffinityMatched }} matched</span>{{ end }}
        </div>

        <div class="delta-strip-detail">{{ template "delta_strip" .Deltas }}</div>

        {{ if .Message }}{{ $reason := .ReasonBody }}
        <div class="reason-panel"><div class="reason-label">Reason</div>{{ if $reason.Headline }}
          <div class="reason-headline">{{ template "reason_segments" $reason.Headline }}</div>
          {{- range $reason.Blocks }}
          <div class="reason-block">
            {{- range .Parts }}
            {{- if .Heading }}
            <div class="reason-block-heading">{{ template "reason_segments" .Heading }}</div>
            {{- else if .Items }}
            <ul class="reason-list">
              {{- range .Items }}
              <li>{{ template "reason_segments" . }}</li>
              {{- end }}
            </ul>
            {{- else }}
            <p class="reason-para">{{ range $i, $line := .Lines }}{{ if $i }}<br>{{ end }}{{ template "reason_segments" $line }}{{ end }}</p>
            {{- end }}
            {{- end }}
          </div>
          {{- end }}
        {{ else }}<div class="reason-text">{{ template "reason_segments" $reason.Plain }}</div>{{ end }}</div>
        {{ end }}

        {{- if .RegressedTasks }}
        <h2 style="font-size:17px;font-weight:600;margin:20px 0 8px">Regressed tasks <span style="font-size:13px;font-weight:500;color:var(--muted)">{{ len .RegressedTasks }} of {{ .EvaluatedTasks }}</span></h2>
        <div style="overflow-x:auto">
          <table class="ci">
            <thead>
              <tr>
                <th>task</th>
                <th>base → candidate</th>
                <th>Δ</th>
                <th>95% CI</th>
              </tr>
            </thead>
            <tbody>
              {{- range .RegressedTasks }}
              <tr>
                <td class="mono" style="font-weight:600">{{ .ID }}</td>
                <td class="num">{{ .BaseRate }} → {{ .CandRate }}</td>
                <td class="num">{{ .Delta }}</td>
                <td class="num" style="font-size:13px;color:var(--muted)">{{ .CILabel }}</td>
              </tr>
              {{- end }}
            </tbody>
          </table>
        </div>
        {{- end }}

        {{ if or .TotalCostStr .TotalTokensStr .BudgetStr }}
        <div class="meta-grid" style="margin-top:16px">
          <div class="meta-tile">
            <div class="label">Total Spent</div>
            <div class="value num">{{ if .TotalCostStr }}{{ .TotalCostStr }}{{ else }}—{{ end }}</div>
          </div>{{ if .BudgetStr }}
          <div class="meta-tile">
            <div class="label">Budget</div>
            <div class="value num"{{ if .BudgetNote }} title="{{ .BudgetNote }}"{{ end }}>{{ .BudgetStr }}</div>
          </div>{{ end }}
          <div class="meta-tile">
            <div class="label">Candidate Tokens</div>
            <div class="value num">{{ if .TotalTokensStr }}{{ .TotalTokensStr }}{{ else }}—{{ end }}</div>
          </div>
          <div class="meta-tile">
            <div class="label">Samples Paired</div>
            <div class="value num">{{ .TotalPairs }}</div>
          </div>
        </div>
        {{ end }}

        <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;margin:16px 0 8px;gap:12px">
          <h2 style="font-size:17px;font-weight:600;margin:0">Task Comparisons</h2>
          <div class="metric-filter-bar" style="margin:0">
            <span class="filter-label">View:</span>
            <a href="{{ metricLink .LinkNamespace "all" }}" class="filter-chip {{ if or (eq .Metric "all") (eq .Metric "") }}active{{ end }}">All Metrics</a>
            <a href="{{ metricLink .LinkNamespace "rate" }}" class="filter-chip {{ if eq .Metric "rate" }}active{{ end }}">Accuracy / Rate</a>
            <a href="{{ metricLink .LinkNamespace "latency" }}" class="filter-chip {{ if eq .Metric "latency" }}active{{ end }}">Latency (p50/p95)</a>
            <a href="{{ metricLink .LinkNamespace "tokens" }}" class="filter-chip {{ if eq .Metric "tokens" }}active{{ end }}">Tokens</a>
            <a href="{{ metricLink .LinkNamespace "cost" }}" class="filter-chip {{ if eq .Metric "cost" }}active{{ end }}">Cost ($ USD)</a>
          </div>
        </div>

        {{ template "comparison_table" . }}
        {{- if .Actions }}
        <ul style="margin:16px 0 0;padding-left:20px;font-size:14px;color:var(--ink-2)">
          {{ range .Actions }}
          <li>run {{ .SuggestedNext }} more samples{{ if and .ShowTaskID .TaskID }} ({{ .TaskID }}){{ end }}</li>
          {{ end }}
        </ul>
        {{- end }}
        {{- if or .Histories .HistoryNote }}
        <h2 style="font-size:17px;font-weight:600;margin:24px 0 12px">Confidence Interval History</h2>
        {{- if .HistoryNote }}
        <div style="font-size:13px;color:var(--muted);padding:8px 0">{{ .HistoryNote }}</div>
        {{- end }}
        {{- range .Histories }}
        <div style="margin-top:16px">
          <div class="mono" style="font-weight:600;font-size:14px;margin-bottom:8px">{{ .TaskID }}</div>
          <div style="overflow-x:auto">
            <svg viewBox="{{ .SVG.ViewBox }}" width="100%" height="220" style="max-width:640px;display:block">
              <!-- Grid / Axes -->
              <line x1="{{ .SVG.X0 }}" y1="{{ .SVG.Y1 }}" x2="{{ .SVG.X1 }}" y2="{{ .SVG.Y1 }}" stroke="var(--line)" stroke-width="1"/>
              <line x1="{{ .SVG.X0 }}" y1="{{ .SVG.Y0 }}" x2="{{ .SVG.X0 }}" y2="{{ .SVG.Y1 }}" stroke="var(--line)" stroke-width="1"/>

              <!-- Equivalence margin reference line -->
              <line x1="{{ .SVG.X0 }}" y1="{{ .SVG.MarginY }}" x2="{{ .SVG.X1 }}" y2="{{ .SVG.MarginY }}" stroke="var(--muted)" stroke-dasharray="4 4" stroke-width="1"/>
              <text x="{{ .SVG.X1 }}" y="{{ .SVG.MarginY }}" dx="-4" dy="-4" text-anchor="end" font-size="11" fill="var(--muted)" font-family="var(--sans)">{{ .SVG.MarginLabel }}</text>

              <!-- Polyline series -->
              <polyline points="{{ .SVG.PointsAttr }}" fill="none" stroke="var(--indigo)" stroke-width="2"/>

              <!-- Points -->
              {{ range .SVG.Circles }}
              <circle cx="{{ .CX }}" cy="{{ .CY }}" r="{{ .R }}" fill="var(--indigo)" stroke="#fff" stroke-width="2"/>
              {{ end }}

              <!-- Final point label -->
              <text x="{{ .SVG.FinalLabel.X }}" y="{{ .SVG.FinalLabel.Y }}" font-size="11" font-weight="600" fill="var(--indigo)" font-family="var(--mono)">{{ .SVG.FinalLabel.Text }}</text>

              <!-- X Ticks & Labels -->
              {{ range .SVG.XAxisTicks }}
              <line x1="{{ .X }}" y1="{{ .Y1 }}" x2="{{ .X }}" y2="{{ .Y2 }}" stroke="var(--line)" stroke-width="1"/>
              <text x="{{ .X }}" y="{{ .TextY }}" text-anchor="middle" font-size="11" fill="var(--muted)" font-family="var(--mono)">{{ .Text }}</text>
              {{ end }}

              <!-- Y Ticks & Labels -->
              {{ range .SVG.YAxisTicks }}
              <text x="{{ .X }}" y="{{ .Y }}" text-anchor="end" font-size="11" fill="var(--muted)" font-family="var(--mono)">{{ .Text }}</text>
              {{ end }}
            </svg>
          </div>

          <div style="overflow-x:auto;margin-top:12px;margin-bottom:20px">
            <table class="ci history-table">
              <thead>
                <tr>
                  <th>n</th>
                  <th>Δ</th>
                  <th>95% CI</th>
                  <th>decision</th>
                </tr>
              </thead>
              <tbody>
                {{ range .Points }}
                <tr>
                  <td class="num">{{ .N }}</td>
                  <td class="num">{{ .DeltaFormatted }}</td>
                  <td class="num">{{ .CIFormatted }}</td>
                  <td><span class="chip {{ .DecisionChip }}"><span class="dot"></span>{{ .Decision }}</span></td>
                </tr>
                {{ end }}
              </tbody>
            </table>
          </div>
        </div>
        {{- end }}
        {{- end }}
        {{- if .Batches }}
        <h2 style="font-size:17px;font-weight:600;margin:24px 0 12px">Batches</h2>
        <div style="overflow-x:auto">
          <table class="ci">
            <thead>
              <tr>
                <th>batch</th>
                <th>phase</th>
                <th>samples</th>
                <th>candidate run</th>
                <th>baseline run</th>
              </tr>
            </thead>
            <tbody>
              {{ range .Batches }}
              <tr>
                <td class="mono" style="font-weight:600">batch {{ .Index }}</td>
                <td><span class="chip {{ .PhaseChip }}"><span class="dot"></span>{{ .Phase }}</span></td>
                <td class="num">{{ if .SampleRange }}{{ .SampleRange }}{{ else }}—{{ end }}</td>
                <td><a href="{{ runLink $.Repo .CandidateRun $.LinkNamespace $.Namespace }}" class="mono" style="font-size:13px">{{ .CandidateRun }}</a></td>
                <td><a href="{{ runLink $.Repo .BaselineRun $.LinkNamespace $.Namespace }}" class="mono" style="font-size:13px">{{ .BaselineRun }}</a></td>
              </tr>
              {{ end }}
            </tbody>
          </table>
        </div>
        {{- end }}
      </div>

      <div class="card-footer">
        <span><b>Base:</b> commit <span class="mono">{{ if .BaseCommitShort }}{{ .BaseCommitShort }}{{ else }}—{{ end }}</span> (<a href="{{ runLink .Repo .BaseRun .LinkNamespace .Namespace }}">{{ .BaseRun }}</a>)</span>
        <span><b>Candidate:</b> commit <span class="mono">{{ if .CandCommitShort }}{{ .CandCommitShort }}{{ else }}—{{ end }}</span> (<a href="{{ runLink .Repo .CandRun .LinkNamespace .Namespace }}">{{ .CandRun }}</a>)</span>
        <span><b>Samples:</b> {{ if .SamplesSummary }}{{ .SamplesSummary }}{{ else }}{{ .TotalSamples }} run · {{ .TotalPairs }} paired{{ if or (gt .TotalDroppedCand 0) (gt .TotalDroppedBase 0) }} · dropped: {{ .TotalDroppedCand }} cand, {{ .TotalDroppedBase }} base{{ end }}{{ end }}</span>
        {{- if .Batches }}
        <span><b>Batches:</b> {{ len .Batches }}</span>
        {{- end }}
        {{ if .SummaryPath }}
        <span style="font-size:12px" class="mono" title="{{ .SummaryPath }}">{{ .SummaryPath }}</span>
        {{ end }}
        {{- if .Version }}
        <span>eval-runner {{ .Version }}</span>
        {{- end }}
      </div>
    </div>
  </main>
</body>
</html>
`

const trajectoryCSS = `
.breadcrumb { font-size: 13px; color: var(--muted); margin-bottom: 14px; }
.breadcrumb a { color: var(--indigo); text-decoration: none; }
.breadcrumb a:hover { text-decoration: underline; }
.sample-link { display: inline-flex; align-items: center; gap: 8px; color: var(--ink); text-decoration: none; font-weight: 500; }
.sample-link:hover { text-decoration: underline; color: var(--indigo); }
.st { width: 8px; height: 8px; border-radius: 50%; display: inline-block; flex-shrink: 0; }
.st.p { background: var(--good); }
.st.f { background: var(--bad); }
.preview-cell { max-width: 320px; }
.preview-text { font-family: var(--mono); font-size: 12px; color: var(--ink-2); white-space: pre-wrap; word-break: break-word; line-height: 1.4; margin: 0; max-height: 4.2em; overflow: hidden; }
.btn-page { font-size: 13px; padding: 4px 10px; border: 1px solid var(--line); border-radius: 4px; color: var(--ink); text-decoration: none; background: var(--surface); }
.btn-page:hover { background: var(--canvas); }
.ev { display: flex; gap: 18px; padding: 12px 16px; border-radius: 10px; margin-bottom: 8px; font-size: 14px; align-items: flex-start; background: var(--canvas); border: 1px solid var(--line); }
.ev .k { width: 100px; flex: none; font-family: var(--mono); font-size: 12px; color: var(--muted); padding-top: 2px; text-transform: uppercase; letter-spacing: 0.05em; }
.ev .b { flex: 1; font-family: var(--mono); font-size: 13px; white-space: pre-wrap; word-break: break-word; line-height: 1.5; margin: 0; color: var(--ink); }
.ev.tool { background: var(--indigo-soft); border-color: #C7D2FE; }
.ev.tool .k { color: var(--indigo); }
.ev.bad { background: var(--bad-soft); border-color: #FECACA; }
.ev.bad .k { color: var(--bad); }
.note-cell { max-width: 240px; }
.ev.note { background: var(--surface); }
.ev.note .k { width: 120px; color: var(--ink-2); }
`

const taskSamplesListTemplate = `
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ .TaskID }} · Samples · eval-runner</title>
  <style>` + baseCSS + trajectoryCSS + `</style>
</head>
<body>{{ template "repo_header" . }}

  <main class="container">
    <div class="card">
      <div class="breadcrumb">
        {{- if .Repo.Host }}
        <a href="{{ repoLink .Repo "/runs" .LinkNamespace }}">Runs</a> / <a href="{{ repoLink .Repo (printf "/runs/%s" .RunName) .LinkNamespace }}">{{ .RunName }}</a> / <span class="mono">{{ .TaskID }}</span>
        {{- else }}
        <a href="/runs">Runs</a> / <a href="/runs/{{ .Namespace }}/{{ .RunName }}">{{ .RunName }}</a> / <span class="mono">{{ .TaskID }}</span>
        {{- end }}
      </div>

      <div class="card-header" style="align-items:flex-start;flex-direction:column;gap:8px">
        <h1 class="card-title mono" style="font-size:22px">{{ .TaskID }}</h1>
        <div class="meta mono" style="font-size:12px;color:var(--muted)">
          commit <span class="mono">{{ .Commit }}</span> · {{ .Model }} · <span class="log-path mono" title="{{ .LogPath }}">{{ .LogPath }}</span>
        </div>
      </div>

      {{ if .StorageMessage }}
      <div class="msg-alert" style="margin-bottom:16px">
        {{ .StorageMessage }}
      </div>
      {{ else if .IsTruncatedLog }}
      <div class="msg-alert" style="margin-bottom:16px">
        <strong>Log Truncated:</strong> {{ .TruncatedMessage }}
      </div>
      {{ else }}{{ with .WarningMessage }}
      <div class="msg-alert" style="margin-bottom:16px">
        {{ . }}{{ if $.WarningLogPath }}; download it from {{ if $.WarningLogURL }}<a href="{{ $.WarningLogURL }}" target="_blank" rel="noopener noreferrer">{{ $.WarningLogPath }}</a>{{ else }}{{ $.WarningLogPath }}{{ end }}{{ end }}
      </div>{{ end }}
      <table class="table" style="margin-top:16px">
        <thead>
          <tr>
            <th style="width:140px">sample</th>
            <th style="width:90px">score</th>
            <th>input preview</th>
            <th>output preview</th>
            <th>pass rule</th>
            <th>grader reason</th>
            <th style="width:90px">latency</th>
            <th style="width:90px">tokens</th>
          </tr>
        </thead>
        <tbody>
          {{ range .Samples }}
          <tr>
            <td class="mono">
              {{ if .Link }}<a href="{{ .Link }}" class="sample-link">
                <span class="st {{ if eq .Status "pass" }}p{{ else if eq .Status "fail" }}f{{ end }}"></span>
                {{ .ID }}
              </a>{{ else }}<span class="st {{ if eq .Status "pass" }}p{{ else if eq .Status "fail" }}f{{ end }}"></span>
              {{ .ID }}{{ end }}
            </td>
            <td>
              <span class="chip chip-{{ .StatusClass }}">{{ .Score }}</span>
            </td>
            <td class="preview-cell"><pre class="preview-text">{{ .InputPreview }}</pre></td>
            <td class="preview-cell"><pre class="preview-text">{{ .OutputPreview }}</pre></td>
            <td class="note-cell">{{ if .Target }}<pre class="preview-text" title="{{ .Target }}">{{ .TargetPreview }}</pre>{{ else }}<span style="color:var(--muted)">—</span>{{ end }}</td>
            <td class="note-cell">{{ if .Explanation }}<pre class="preview-text" title="{{ .Explanation }}">{{ .ExplanationPreview }}</pre>{{ else }}<span style="color:var(--muted)">—</span>{{ end }}</td>
            <td class="mono num">{{ .LatencyStr }}</td>
            <td class="mono num">{{ .TokensStr }}</td>
          </tr>
          {{ else }}
          <tr>
            <td colspan="8" class="empty-state">No samples recorded for this task.</td>
          </tr>
          {{ end }}
        </tbody>
      </table>

      {{ if or .HasPrev .HasNext }}
      <div class="pagination-bar" style="display:flex;justify-content:space-between;align-items:center;margin-top:16px;padding:8px 0;border-top:1px solid var(--line)">
        <div style="font-size:13px;color:var(--muted)">
          Showing {{ .ShowingStart }}–{{ .ShowingEnd }} of {{ .TotalSamples }} samples
        </div>
        <div style="display:flex;gap:8px">
          {{ if .HasPrev }}
          <a href="{{ .PrevLink }}" class="btn-page">← Previous</a>
          {{ end }}
          {{ if .HasNext }}
          <a href="{{ .NextLink }}" class="btn-page">Next →</a>
          {{ end }}
        </div>
      </div>
      {{ end }}
      {{ end }}

      <div class="card-footer">
        <span>{{ .ArtifactFooter }}</span>
        {{- if .Version }}
        <span>eval-runner {{ .Version }}</span>
        {{- end }}
      </div>
    </div>
  </main>
</body>
</html>
`

const sampleTranscriptTemplate = `
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ .SampleID }} · {{ .TaskID }} · eval-runner</title>
  <style>` + baseCSS + trajectoryCSS + `</style>
</head>
<body>{{ template "repo_header" . }}

  <main class="container">
    <div class="card">
      <div class="breadcrumb">
        {{- if .Repo.Host }}
        <a href="{{ repoLink .Repo "/runs" .LinkNamespace }}">Runs</a> / <a href="{{ repoLink .Repo (printf "/runs/%s" .RunName) .LinkNamespace }}">{{ .RunName }}</a> / <a href="{{ repoLink .Repo (printf "/runs/%s/tasks/%s/samples" .RunName .EscapedTaskID) .LinkNamespace }}">{{ .TaskID }}</a> / <span class="mono">{{ .SampleID }}</span>
        {{- else }}
        <a href="/runs">Runs</a> / <a href="/runs/{{ .Namespace }}/{{ .RunName }}">{{ .RunName }}</a> / <a href="/runs/{{ .Namespace }}/{{ .RunName }}/tasks/{{ .EscapedTaskID }}/samples">{{ .TaskID }}</a> / <span class="mono">{{ .SampleID }}</span>
        {{- end }}
      </div>

      <div class="card-header" style="align-items:flex-start;flex-direction:column;gap:8px">
        <h2 style="font-size:20px;font-weight:600;margin:0">
          <span class="mono">{{ .SampleID }}</span> · <span class="chip chip-{{ .StatusClass }}">{{ .StatusUpper }}</span> · {{ .LatencyStr }} · {{ .TokensStr }} tokens
        </h2>
        <div class="meta mono" style="font-size:12px;color:var(--muted)">
          <span class="log-path mono" title="{{ .LogPath }}">{{ .LogPath }}</span>
        </div>
      </div>

      {{ if .StorageMessage }}
      <div class="msg-alert" style="margin-bottom:16px">
        {{ .StorageMessage }}
      </div>
      {{ else if .IsTruncatedLog }}
      <div class="msg-alert" style="margin-bottom:16px">
        <strong>Log Truncated:</strong> {{ .TruncatedMessage }}
      </div>
      {{ else }}

      {{- if or .Target .Explanation }}
      <div class="eval-notes" style="margin-top:20px">
        {{- if .Target }}
        <div class="ev note">
          <span class="k">pass rule</span>
          <pre class="b">{{ .Target }}</pre>
        </div>
        {{- end }}
        {{- if .Explanation }}
        <div class="ev note">
          <span class="k">grader reason</span>
          <pre class="b">{{ .Explanation }}</pre>
        </div>
        {{- end }}
      </div>
      {{- end }}

      <div class="transcript" style="margin-top:20px">
        {{ range .Rows }}
        <div class="ev {{ .Class }}">
          <span class="k">{{ .Kind }}</span>
          <pre class="b">{{ .Content }}</pre>
        </div>
        {{ else }}
        <div class="empty-state">No transcript events recorded for this sample.</div>
        {{ end }}

        {{ if .IsEventsTruncated }}
        <div class="truncation-marker mono" style="margin-top:12px;padding:10px 14px;background:var(--neutral-soft);color:var(--muted);border-radius:6px;font-size:13px;text-align:center">
          {{ .EventsTruncatedMessage }}
        </div>
        {{ end }}
      </div>
      {{ end }}

      <div class="card-footer">
        <span>{{ .ArtifactFooter }}</span>
        {{- if .Version }}
        <span>eval-runner {{ .Version }}</span>
        {{- end }}
      </div>
    </div>
  </main>
</body>
</html>
`

var funcMap = template.FuncMap{
	"repoLink":      repoLink,
	"repoIndexLink": repoIndexLink,
	"runLink":       runLink,
	"metricLink": func(linkNS, metric string) string {
		v := url.Values{}
		v.Set("metric", metric)
		if linkNS != "" {
			v.Set("namespace", linkNS)
		}
		return "?" + v.Encode()
	},
}

func parseTemplate(name, text string) *template.Template {
	t := template.New(name).Funcs(funcMap)
	template.Must(t.Parse(repoHeaderPartial))
	return template.Must(t.Parse(text))
}

func parseComparisonsListTemplate(name, text string) *template.Template {
	t := template.New(name).Funcs(funcMap)
	template.Must(t.Parse(repoHeaderPartial))
	template.Must(t.Parse(comparisonCellTemplate))
	template.Must(t.Parse(deltaStripTemplate))
	return template.Must(t.Parse(text))
}

func parseComparisonDetailTemplate(name, text string) *template.Template {
	t := template.New(name).Funcs(funcMap)
	template.Must(t.Parse(repoHeaderPartial))
	template.Must(t.Parse(ComparisonTableTemplate))
	template.Must(t.Parse(deltaStripTemplate))
	template.Must(t.Parse(reasonSegmentsTemplate))
	return template.Must(t.Parse(text))
}

var (
	tmplRunsList         = parseTemplate("runs_list", runsListTemplate)
	tmplRunDetail        = parseTemplate("run_detail", runDetailTemplate)
	tmplComparisonsList  = parseComparisonsListTemplate("comparisons_list", comparisonsListTemplate)
	tmplComparisonTable  = template.Must(template.New("comparison_table").Funcs(funcMap).Parse(ComparisonTableTemplate))
	tmplComparisonDetail = parseComparisonDetailTemplate("comparison_detail", comparisonDetailTemplate)
	tmplTaskSamplesList  = parseTemplate("task_samples_list", taskSamplesListTemplate)
	tmplSampleTranscript = parseTemplate("sample_transcript", sampleTranscriptTemplate)
	tmplPullsList        = parseTemplate("pulls_list", pullsListTemplate)
	tmplPullDetail       = parseTemplate("pull_detail", pullDetailTemplate)
	tmplSeriesList       = parseTemplate("series_list", seriesListTemplate)
	tmplSeriesDetail     = parseTemplate("series_detail", seriesDetailTemplate)
	tmplRepoIndex        = parseTemplate("repo_index", repoIndexTemplate)
	tmplRepoOverview     = parseTemplate("repo_overview", repoOverviewTemplate)
	tmplRepoSuiteDetail  = parseTemplate("repo_suite_detail", repoSuiteDetailTemplate)

	// TmplComparisonTable is the parsed template for the comparison table sub-template.
	TmplComparisonTable = tmplComparisonTable
)

const pullsListTemplate = `
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Pull Requests · eval-runner</title>
  <style>` + baseCSS + `
.state-filter { color: var(--muted); text-decoration: none; }
.state-filter:hover { color: var(--ink); text-decoration: underline; }
.state-filter.active { color: var(--ink); font-weight: 600; text-decoration: none; }
.chip.open { background: var(--good-soft); color: var(--good); }
.chip.merged { background: #F3E8FF; color: #7E22CE; }
.chip.closed { background: var(--bad-soft); color: var(--bad); }
</style>
</head>
<body>
  {{ template "repo_header" . }}

  <main class="container">
    <div class="card">
      <div class="card-header">
        <h1 class="card-title">Pull Requests</h1>
        <div style="font-size:13px;display:flex;align-items:center;gap:6px">
          <a href="{{ .OpenLink }}" class="state-filter{{ if eq .State "open" }} active{{ end }}">Open {{ .OpenCount }}</a>
          <span style="color:var(--muted)">·</span>
          <a href="{{ .ClosedLink }}" class="state-filter{{ if eq .State "closed" }} active{{ end }}">Closed {{ .ClosedCount }}</a>
          <span style="color:var(--muted)">·</span>
          <a href="{{ .AllLink }}" class="state-filter{{ if or (eq .State "all") (eq .State "") }} active{{ end }}">All</a>
        </div>
      </div>
      <div style="overflow-x:auto">
        <table class="ci">
          <thead>
            <tr>
              {{- if not .Repo.Host }}
              <th>Repository</th>
              {{- end }}
              <th>PR</th>
              <th>Base → Head</th>
              <th>Verdict</th>
              <th>Samples</th>
              <th>Batches</th>
              <th>Completed</th>
              <th>Age</th>
              <th>Comparison</th>
            </tr>
          </thead>
          <tbody>
            {{ range .Pulls }}
            <tr>
              {{- if not $.Repo.Host }}
              <td><a href="{{ .RepoLink }}" style="font-weight:600">{{ .Repository }}</a></td>
              {{- end }}
              <td><a href="{{ .HistoryLink }}" style="font-weight:600">#{{ .Number }}</a> <span class="chip {{ .StateChip }}"><span class="dot"></span>{{ .State }}</span> <a href="{{ .GitHubURL }}" target="_blank" rel="noopener" style="font-size:12px;color:var(--muted);margin-left:4px">GitHub ↗</a></td>
              <td><span class="mono" style="font-size:13px">{{ if .BaseSHAShort }}{{ .BaseSHAShort }} → {{ end }}{{ .HeadSHAShort }}</span></td>
              <td><span class="chip {{ .VerdictChip }}"><span class="dot"></span>{{ .Verdict }}</span>{{ if ne .PImprovement "—" }} <span class="mono" style="font-size:12px;color:var(--muted)">(P={{ .PImprovement }})</span>{{ end }}</td>
              <td class="num">{{ .SamplesRun }}</td>
              <td class="num">{{ .Batches }}</td>
              <td style="color:var(--muted);font-size:13px">{{ .CompletionTime }}</td>
              <td style="color:var(--muted);font-size:13px">{{ .Age }}</td>
              <td><a href="{{ .ComparisonLink }}" class="mono" style="font-size:13px">{{ .ComparisonName }}</a></td>
            </tr>
            {{ else }}
            <tr>
              <td colspan="{{ if .Repo.Host }}8{{ else }}9{{ end }}" class="empty-state">No pull requests found.</td>
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

const pullDetailTemplate = `
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ .Repository }} #{{ .Number }} · Pull Request · eval-runner</title>
  <style>` + baseCSS + `</style>
</head>
<body>
  {{ template "repo_header" . }}

  <main class="container">
    <div class="card">
      <div class="card-header">
        <div style="display:flex;align-items:center;gap:12px">
          <h1 class="card-title">{{ if not .Repo.Host }}{{ .Repository }} {{ end }}<a href="{{ .GitHubURL }}" target="_blank" rel="noopener">#{{ .Number }}</a></h1>
        </div>
        <div style="font-size:13px;color:var(--muted)"><a href="{{ .BackLink }}">← Back to Pull Requests</a></div>
      </div>
      <div class="card-body">
        <div style="overflow-x:auto">
          <table class="ci">
            <thead>
              <tr>
                <th>Comparison</th>
                <th>Head</th>
                <th>Base</th>
                <th>Candidate Run</th>
                <th>Baseline Run</th>
                <th>Verdict</th>
                <th>P(imp)</th>
                <th>Samples</th>
                <th>Batches</th>
                <th>Completed</th>
                <th>Age</th>
              </tr>
            </thead>
            <tbody>
              {{ range .Comparisons }}
              <tr>
                <td><a href="{{ .Link }}" class="mono" style="font-weight:600">{{ .Name }}</a></td>
                <td><span class="mono" style="font-size:13px">{{ .HeadSHAShort }}</span></td>
                <td><span class="mono" style="font-size:13px">{{ .BaseSHAShort }}</span></td>
                <td>{{ if .CandidateRun }}<a href="{{ .CandidateLink }}" class="mono" style="font-size:13px">{{ .CandidateRun }}</a>{{ else }}<span style="color:var(--muted)">—</span>{{ end }}</td>
                <td>{{ if .BaselineRun }}<a href="{{ .BaselineLink }}" class="mono" style="font-size:13px">{{ .BaselineRun }}</a>{{ else }}<span style="color:var(--muted)">—</span>{{ end }}</td>
                <td><span class="chip {{ .VerdictChip }}"><span class="dot"></span>{{ .Verdict }}</span></td>
                <td class="num">{{ .PImprovement }}</td>
                <td class="num">{{ .SamplesRun }}</td>
                <td class="num">{{ .Batches }}</td>
                <td style="color:var(--muted);font-size:13px">{{ .CompletionTime }}</td>
                <td style="color:var(--muted);font-size:13px">{{ .Age }}</td>
              </tr>
              {{ else }}
              <tr>
                <td colspan="11" class="empty-state">No comparisons recorded for this pull request.</td>
              </tr>
              {{ end }}
            </tbody>
          </table>
        </div>
      </div>
      <div class="card-footer">
        {{- if not .Repo.Host }}
        <span><b>Repository:</b> {{ .Repository }}</span>
        {{- end }}
        <span><b>PR:</b> <a href="{{ .GitHubURL }}" target="_blank" rel="noopener">#{{ .Number }}</a></span>
        <span><b>Comparisons:</b> {{ .TotalCount }}</span>
        {{- if .Version }}
        <span>eval-runner {{ .Version }}</span>
        {{- end }}
      </div>
    </div>
  </main>
</body>
</html>
`

// RenderPullsList renders the pull requests list template to the given writer.
func RenderPullsList(w io.Writer, view PullRequestsListView) error {
	return tmplPullsList.Execute(w, view)
}

// RenderPullDetail renders the pull request detail template to the given writer.
func RenderPullDetail(w io.Writer, view PullRequestDetailView) error {
	return tmplPullDetail.Execute(w, view)
}

// RenderTaskSamplesList renders the task samples list template to the given writer.
func RenderTaskSamplesList(w io.Writer, view TaskSamplesListView) error {
	return tmplTaskSamplesList.Execute(w, view)
}

// RenderSampleTranscript renders the sample transcript template to the given writer.
func RenderSampleTranscript(w io.Writer, view SampleTranscriptView) error {
	return tmplSampleTranscript.Execute(w, view)
}

// RenderComparisonTable renders the comparison table sub-template to the given writer.
func RenderComparisonTable(w io.Writer, view ComparisonTableView) error {
	if view.Metric == "" {
		view.Metric = MetricFilterAll
	}
	if !view.HasTokens {
		for _, t := range view.Tasks {
			if t.Tokens != nil {
				view.HasTokens = true
				break
			}
		}
	}
	if !view.HasLatency {
		for _, t := range view.Tasks {
			if t.LatencyStr != "" || t.CandLatencyStr != "" || t.BaseLatencyStr != "" {
				view.HasLatency = true
				break
			}
		}
	}
	if !view.HasCost {
		for _, t := range view.Tasks {
			if t.CostStr != "" || t.CandCostStr != "" || t.BaseCostStr != "" {
				view.HasCost = true
				break
			}
		}
	}
	return tmplComparisonTable.ExecuteTemplate(w, "comparison_table", view)
}

const seriesListTemplate = `
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Series · eval-runner</title>
  <style>` + baseCSS + `</style>
</head>
<body>
  <header class="header-bar">
    <div class="header-brand">
      <a href="/runs" class="header-logo">{{ template "wordmark" }}</a>
      <nav class="nav-links">
        <a href="/pulls" class="nav-link {{ if eq .ActiveTab "pulls" }}active{{ end }}">Pull Requests</a>
        <a href="/runs" class="nav-link {{ if eq .ActiveTab "runs" }}active{{ end }}">Runs</a>
        <a href="/comparisons" class="nav-link {{ if eq .ActiveTab "comparisons" }}active{{ end }}">Comparisons</a>
        <a href="/series" class="nav-link {{ if eq .ActiveTab "series" }}active{{ end }}">Series</a>
      </nav>
    </div>
    {{- if .LinkNamespace }}
    <span class="badge chip neutral mono" style="font-size:12px">namespace: {{ .LinkNamespace }}</span>
    {{- end }}
  </header>

  <main class="container">
    <div class="card">
      <div class="card-header">
        <h1 class="card-title">Main-Branch Series</h1>
        <span style="font-size:13px;color:var(--muted)">{{ .TotalCount }} total</span>
      </div>
      <div style="overflow-x:auto">
        <table class="ci">
          <thead>
            <tr>
              <th>Repository</th>
              <th>Branch</th>
              <th>Runs</th>
              <th>Last Move</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            {{ range .Series }}
            <tr>
              <td><a href="{{ .Link }}" style="font-weight:600">{{ .Repository }}</a></td>
              <td><span class="mono" style="font-size:13px">{{ .Branch }}</span></td>
              <td class="num">{{ .RunCount }}</td>
              <td class="num" style="font-size:13px">{{ if .LastMove }}<span class="chip {{ .LastMoveChip }}" title="{{ .LastMoveMethod }}">{{ .LastMoveStr }}</span>{{ else }}—{{ end }}</td>
              <td><a href="{{ .Link }}" class="mono" style="font-size:13px">view series →</a></td>
            </tr>
            {{ else }}
            <tr>
              <td colspan="5" class="empty-state">No series found.</td>
            </tr>
            {{ end }}
          </tbody>
        </table>
      </div>
    </div>
  </main>
</body>
</html>
`

const seriesDetailTemplate = `
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ .Repository }} ({{ .Branch }}) · Series · eval-runner</title>
  <style>` + baseCSS + `</style>
</head>
<body>
{{- if .Repo.Host }}
{{ template "repo_header" . }}
{{- else }}
  <header class="header-bar">
    <div class="header-brand">
      <a href="/runs" class="header-logo">{{ template "wordmark" }}</a>
      <nav class="nav-links">
        <a href="/pulls" class="nav-link {{ if eq .ActiveTab "pulls" }}active{{ end }}">Pull Requests</a>
        <a href="/runs" class="nav-link {{ if eq .ActiveTab "runs" }}active{{ end }}">Runs</a>
        <a href="/comparisons" class="nav-link {{ if eq .ActiveTab "comparisons" }}active{{ end }}">Comparisons</a>
        <a href="/series" class="nav-link {{ if eq .ActiveTab "series" }}active{{ end }}">Series</a>
      </nav>
    </div>
    {{- if .LinkNamespace }}
    <span class="badge chip neutral mono" style="font-size:12px">namespace: {{ .LinkNamespace }}</span>
    {{- end }}
  </header>
{{- end }}

  <main class="container">
    <div class="card">
      <div class="card-header">
        <div>
          <h1 class="card-title">
            {{- if not .Repo.Host }}
            <span>{{ .Repository }}</span>
            {{- end }}
            <span class="mono" style="font-size:16px;color:var(--muted);font-weight:normal">({{ .Branch }})</span>
          </h1>
          <div style="font-size:13px;color:var(--muted);margin-top:4px"><a href="{{ .BackLink }}">← Back to Series</a></div>
        </div>
        <span style="font-size:13px;color:var(--muted)">showing last {{ len .Items }} of {{ .TotalRuns }} commits</span>
      </div>
      <div class="card-body">
        {{- if .OrderedNote }}
        <div style="font-size:13px;color:var(--muted);margin-bottom:12px">{{ .OrderedNote }}</div>
        {{- end }}
        {{- if or .CohortsLink .Tasks }}
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:16px">
          {{- if .CohortsLink }}
          <a href="{{ .CohortsLink }}" class="chip {{ if .IsCohortsView }}good{{ else }}neutral{{ end }} mono" style="font-size:12px">Cohorts</a>
          {{- end }}
          {{- if .Tasks }}
          <span style="font-size:13px;font-weight:600;color:var(--muted)">Task:</span>
          {{ range .Tasks }}
          <a href="{{ .Link }}" class="chip {{ if and (not $.IsCohortsView) .Active }}good{{ else }}neutral{{ end }} mono" style="font-size:12px">{{ .ID }}</a>
          {{ end }}
          {{- end }}
        </div>
        {{- end }}
        {{- if .IsCohortsView }}
        <div class="mono" style="font-weight:600;font-size:15px;margin-bottom:4px">Cohorts</div>
        <div style="font-size:13px;color:var(--muted);margin-bottom:12px">Mean score per cohort over commits · 95% CI shaded · pooled Wilson</div>
        {{- if .CohortLegend }}
        <div style="display:flex;align-items:center;gap:16px;flex-wrap:wrap;margin-bottom:16px;font-size:13px">
          {{ range .CohortLegend }}
          <span style="display:inline-flex;align-items:center;gap:6px">
            <svg width="12" height="12" aria-hidden="true"><rect width="12" height="12" rx="2" fill="{{ .Color }}"/></svg>
            <span style="font-weight:600">{{ .Name }}</span>
            <span style="color:var(--muted)">({{ .Count }} {{ if eq .Count 1 }}eval{{ else }}evals{{ end }})</span>
          </span>
          {{ end }}
        </div>
        {{- end }}
        {{- else }}
        {{- if .SelectedTask }}
        <div class="mono" style="font-weight:600;font-size:15px;margin-bottom:12px">{{ .SelectedTask }}</div>
        {{- end }}
        {{- end }}

        <div style="overflow-x:auto">
          <svg viewBox="{{ .SVG.ViewBox }}" width="100%" height="220" style="max-width:640px;display:block">
            <!-- Y-Axis Grid Lines & Labels -->
            {{ range .SVG.YAxisTicks }}
            <line x1="{{ $.SVG.X0 }}" y1="{{ .Y1 }}" x2="{{ $.SVG.X1 }}" y2="{{ .Y1 }}" stroke="var(--line)" stroke-width="1" stroke-dasharray="3,3"/>
            <text x="{{ .X }}" y="{{ .Y }}" text-anchor="end" font-size="11" fill="var(--muted)" font-family="var(--mono)">{{ .Text }}</text>
            {{ end }}

            <!-- Axes -->
            <line x1="{{ .SVG.X0 }}" y1="{{ .SVG.Y1 }}" x2="{{ .SVG.X1 }}" y2="{{ .SVG.Y1 }}" stroke="var(--line)" stroke-width="1"/>
            <line x1="{{ .SVG.X0 }}" y1="{{ .SVG.Y0 }}" x2="{{ .SVG.X0 }}" y2="{{ .SVG.Y1 }}" stroke="var(--line)" stroke-width="1"/>

            <!-- Shaded CI Polygons -->
            {{ range $i, $poly := .SVG.Polygons }}
            <polygon points="{{ $poly }}" fill="{{ if $.SVG.PolygonFills }}{{ index $.SVG.PolygonFills $i }}{{ else }}var(--indigo){{ end }}" fill-opacity="0.15" stroke="none"/>
            {{ end }}
            {{- if .SVG.Annotations }}

            <!-- Annotations -->
            {{ range .SVG.Annotations }}
            <line x1="{{ .X }}" y1="{{ .Y1 }}" x2="{{ .X }}" y2="{{ .Y2 }}" stroke="{{ if .Stroke }}{{ .Stroke }}{{ else }}var(--teal){{ end }}" stroke-dasharray="4,4" stroke-width="1.5"/>
            <text x="{{ .TextX }}" y="{{ .TextY }}" fill="{{ if .Stroke }}{{ .Stroke }}{{ else }}var(--teal){{ end }}" font-size="10" font-family="var(--sans)" font-weight="600"{{ if .TextAnchor }} text-anchor="{{ .TextAnchor }}"{{ end }}>{{ .Text }}</text>
            {{ end }}
            {{- end }}

            <!-- Polylines -->
            {{ range .SVG.Polylines }}
            <polyline points="{{ .Points }}" fill="none" stroke="{{ if .Stroke }}{{ .Stroke }}{{ else }}var(--indigo){{ end }}" stroke-width="2"/>
            {{ end }}

            <!-- Data Points -->
            {{ range .SVG.Circles }}
            <circle cx="{{ .CX }}" cy="{{ .CY }}" r="{{ .R }}" fill="{{ if .Fill }}{{ .Fill }}{{ else }}var(--indigo){{ end }}" stroke="#fff" stroke-width="2"/>
            {{ end }}

            <!-- X-Axis Ticks & Labels -->
            {{ range .SVG.XAxisTicks }}
            <line x1="{{ .X }}" y1="{{ .Y1 }}" x2="{{ .X }}" y2="{{ .Y2 }}" stroke="var(--line)" stroke-width="1"/>
            <text x="{{ .X }}" y="{{ .TextY }}" text-anchor="middle" font-size="11" fill="var(--muted)" font-family="var(--mono)">{{ .Text }}</text>
            {{ end }}
          </svg>
        </div>
        {{- if .IsCohortsView }}
        <div style="overflow-x:auto;margin-top:20px">
          <table class="ci">
            <thead>
              <tr>
                <th>Cohort</th>
                <th>Evals</th>
                <th>First Seen Commit</th>
                <th>Latest Mean</th>
                <th>95% CI</th>
              </tr>
            </thead>
            <tbody>
              {{ range .CohortRows }}
              <tr>
                <td>
                  <span style="display:inline-flex;align-items:center;gap:6px">
                    <svg width="10" height="10" aria-hidden="true"><rect width="10" height="10" rx="2" fill="{{ .Color }}"/></svg>
                    <span class="mono" style="font-weight:600">{{ .Cohort }}</span>
                  </span>
                </td>
                <td class="num">{{ .EvalsCount }}</td>
                <td><span class="mono" style="font-size:13px">{{ .FirstSeenCommit }}</span></td>
                <td class="num" style="font-size:13px"{{ if .MethodTooltip }} title="{{ .MethodTooltip }}"{{ end }}>{{ .LatestMean }}</td>
                <td class="num" style="font-size:13px">{{ .CIStr }}</td>
              </tr>
              {{ else }}
              <tr>
                <td colspan="5" class="empty-state">No cohort data available.</td>
              </tr>
              {{ end }}
            </tbody>
          </table>
        </div>
        {{- else }}

        <div style="overflow-x:auto;margin-top:20px">
          <table class="ci">
            <thead>
              <tr>
                <th>Commit</th>
                <th>Time</th>
                <th>Phase</th>
                <th>Samples</th>
                <th>Rate</th>
                <th>95% CI</th>
                <th>Δ vs prev (95% CI)</th>
                <th>Tokens</th>
                <th>Cost</th>
                <th>Run</th>
              </tr>
            </thead>
            <tbody>
              {{ range .Items }}
              <tr>
                <td><span class="mono" style="font-size:13px">{{ .CommitSHAShort }}</span></td>
                <td><span style="color:var(--muted);font-size:13px">{{ .CommitTimeStr }}</span></td>
                <td><span class="chip {{ .PhaseChip }}"><span class="dot"></span>{{ if .PhaseLabel }}{{ .PhaseLabel }}{{ else }}{{ .Phase }}{{ end }}</span></td>
                <td class="num">{{ .Samples }}</td>
                <td class="num">{{ .RateStr }}</td>
                <td class="num" style="font-size:13px">{{ .CIStr }}</td>
                <td class="num" style="font-size:13px">{{ if .DeltaPrev }}<span class="chip {{ .DeltaPrevChip }}" title="{{ .DeltaPrevMethod }}">{{ .DeltaPrevStr }}</span>{{ else }}—{{ end }}</td>
                <td class="num">{{ .TokensStr }}</td>
                <td class="num"{{ if .CostTooltip }} title="{{ .CostTooltip }}"{{ end }}>{{ .CostStr }}</td>
                <td><a href="{{ .RunLink }}" class="mono" style="font-size:13px">{{ .RunName }}</a></td>
              </tr>
              {{ else }}
              <tr>
                <td colspan="10" class="empty-state">No runs recorded for this series.</td>
              </tr>
              {{ end }}
            </tbody>
          </table>
        </div>
        {{- end }}
      </div>
      <div class="card-footer">
        {{- if not .Repo.Host }}
        <span><b>Repository:</b> {{ .Repository }}</span>
        {{- end }}
        <span><b>Branch:</b> {{ .Branch }}</span>
        <span><b>Total Runs:</b> {{ .TotalRuns }}</span>
        {{- if .Version }}
        <span>eval-runner {{ .Version }}</span>
        {{- end }}
      </div>
    </div>
  </main>
</body>
</html>
`

// RenderSeriesList renders the series list template to the given writer.
func RenderSeriesList(w io.Writer, view SeriesListView) error {
	return tmplSeriesList.Execute(w, view)
}

// RenderSeriesDetail renders the series detail template to the given writer.
func RenderSeriesDetail(w io.Writer, view SeriesDetailView) error {
	return tmplSeriesDetail.Execute(w, view)
}

// RenderRepoIndex renders the repository index template to the given writer.
func RenderRepoIndex(w io.Writer, view RepoIndexView) error {
	return tmplRepoIndex.Execute(w, view)
}

// RenderRepoOverview renders the repository overview template to the given writer.
func RenderRepoOverview(w io.Writer, view RepoOverviewView) error {
	return tmplRepoOverview.Execute(w, view)
}

// RenderRepoSuiteDetail renders the eval suite detail template to the given writer.
func RenderRepoSuiteDetail(w io.Writer, view RepoSuiteDetailView) error {
	return tmplRepoSuiteDetail.Execute(w, view)
}

const repoSuiteDetailTemplate = `
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ if .Repo.Name }}{{ .Repo.Name }} · {{ end }}{{ .SuiteName }} · Eval Suite · eval-runner</title>
  <style>` + baseCSS + `
.overview-headline {
  display: flex;
  align-items: baseline;
  flex-wrap: wrap;
  gap: 12px 24px;
  margin-bottom: 24px;
  padding: 16px 20px;
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: 8px;
}
.overview-headline .headline-score {
  font-size: 28px;
  font-weight: 700;
  color: var(--ink);
  font-variant-numeric: tabular-nums;
}
.overview-headline .headline-ci {
  font-size: 16px;
  color: var(--muted);
  font-variant-numeric: tabular-nums;
  margin-left: 6px;
}
.overview-headline .headline-meta {
  font-size: 14px;
  color: var(--muted);
  margin-left: auto;
}
.eval-card {
  margin-bottom: 24px;
}
.model-legend {
  display: flex;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
  margin-bottom: 12px;
  font-size: 13px;
}
.model-legend-item {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}
.chart-commit-labels {
  display: flex;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
  font-size: 12px;
  color: var(--muted);
  margin-top: 8px;
  margin-bottom: 16px;
}
.eval-trend-svg {
  display: block;
  max-width: 640px;
}
</style>
</head>
<body>
  {{ template "repo_header" . }}

  <main class="container">
    <div style="margin-bottom:12px;font-size:13px;color:var(--muted)">
      <a href="{{ .OverviewLink }}">← Back to Overview</a>
    </div>

    <!-- 1. Headline Header -->
    <div class="overview-headline">
      <div>
        <span class="headline-score">{{ .SuiteScore }}</span>
        <span class="headline-ci">{{ .SuiteCI }}</span>
        {{- if .HasDelta }}
        <span class="chip {{ .DeltaClass }}" style="margin-left:12px">{{ .DeltaStr }}</span>
        {{- end }}
      </div>
      <div class="headline-meta">
        <span class="mono" style="font-weight:600;font-size:16px;color:var(--ink)">{{ .SuiteName }}</span>
        <span style="color:var(--muted)">·</span>
        <span>{{ .EvalsCount }} {{ if eq .EvalsCount 1 }}eval{{ else }}evals{{ end }}</span>
        {{- if .CurrentCommitShort }}
        <span style="color:var(--muted)">·</span>
        <span>at commit <span class="mono">{{ .CurrentCommitShort }}</span></span>
        {{- end }}
      </div>
    </div>

    <!-- 2 & 3. One section per eval -->
    {{- range .Evals }}
    <div class="card eval-card">
      <div class="card-header" style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px">
        <div style="display:flex;align-items:baseline;gap:12px;flex-wrap:wrap">
          <h2 class="card-title mono" style="margin:0">{{ .ID }}</h2>
          <div>
            <span class="mono" style="font-size:16px;font-weight:600">{{ .ScoreStr }}</span>
            <span style="font-size:12px;color:var(--muted);margin-left:4px">{{ .CIStr }}</span>
          </div>
          {{- if .HasDelta }}
          <span class="chip {{ .DeltaClass }}">{{ .DeltaStr }}</span>
          {{- end }}
        </div>
      </div>
      <div class="card-body">
        {{- if .HasModelBreakdown }}
        <div class="model-legend">
          <span style="font-weight:600;color:var(--muted)">Models:</span>
          {{- range .ModelLegend }}
          <span class="model-legend-item">
            <svg width="12" height="12" aria-hidden="true"><rect width="12" height="12" rx="2" fill="{{ .Color }}"/></svg>
            <span class="mono" style="font-weight:600">{{ .Name }}:</span>
            <span class="mono">{{ .ScoreStr }}</span>
            <span style="font-size:11px;color:var(--muted)">{{ .CIStr }}</span>
          </span>
          {{- end }}
        </div>
        {{- end }}

        {{- $svg := .SVG }}
        <!-- Trend Chart SVG -->
        <div style="overflow-x:auto">
          <svg viewBox="{{ $svg.ViewBox }}" width="100%" height="220" class="eval-trend-svg">
            <!-- Y-Axis Grid Lines & Labels -->
            {{ range $svg.YAxisTicks }}
            <line x1="{{ $svg.X0 }}" y1="{{ .Y1 }}" x2="{{ $svg.X1 }}" y2="{{ .Y1 }}" stroke="var(--line)" stroke-width="1" stroke-dasharray="3,3"/>
            <text x="{{ .X }}" y="{{ .Y }}" text-anchor="end" font-size="11" fill="var(--muted)" font-family="var(--mono)">{{ .Text }}</text>
            {{ end }}

            <!-- Axes -->
            <line x1="{{ $svg.X0 }}" y1="{{ $svg.Y1 }}" x2="{{ $svg.X1 }}" y2="{{ $svg.Y1 }}" stroke="var(--line)" stroke-width="1"/>
            <line x1="{{ $svg.X0 }}" y1="{{ $svg.Y0 }}" x2="{{ $svg.X0 }}" y2="{{ $svg.Y1 }}" stroke="var(--line)" stroke-width="1"/>

            <!-- Shaded CI Polygons -->
            {{ range $i, $poly := $svg.Polygons }}
            <polygon points="{{ $poly }}" fill="{{ if $svg.PolygonFills }}{{ index $svg.PolygonFills $i }}{{ else }}var(--indigo){{ end }}" fill-opacity="0.15" stroke="none"/>
            {{ end }}

            <!-- Polylines -->
            {{ range $svg.Polylines }}
            <polyline points="{{ .Points }}" fill="none" stroke="{{ if .Stroke }}{{ .Stroke }}{{ else }}var(--indigo){{ end }}" stroke-width="2"/>
            {{ end }}

            <!-- Data Points -->
            {{ range $svg.Circles }}
            <circle cx="{{ .CX }}" cy="{{ .CY }}" r="{{ .R }}" fill="{{ if .Fill }}{{ .Fill }}{{ else }}var(--indigo){{ end }}" stroke="#fff" stroke-width="2"/>
            {{ end }}

            <!-- Observation Ticks -->
            {{ range $svg.Ticks }}
            <line x1="{{ .X1 }}" y1="{{ .Y1 }}" x2="{{ .X2 }}" y2="{{ .Y2 }}" stroke="{{ if .Stroke }}{{ .Stroke }}{{ else }}var(--indigo){{ end }}" stroke-width="1.5" stroke-linecap="round" class="obs-tick"/>
            {{ end }}

            <!-- X-Axis Commit Ticks -->
            {{ range $svg.XAxisTicks }}
            <line x1="{{ .X }}" y1="{{ .Y1 }}" x2="{{ .X }}" y2="{{ .Y2 }}" stroke="var(--line)" stroke-width="1"/>
            <text x="{{ .X }}" y="{{ .TextY }}" text-anchor="middle" font-size="10" fill="var(--muted)" font-family="var(--mono)">{{ .Text }}</text>
            {{ end }}
          </svg>
        </div>

        <!-- Commit Labels in text under chart -->
        <div class="chart-commit-labels">
          <span>first: <span class="mono">{{ .FirstCommitShort }}</span></span>
          <span>last: <span class="mono">{{ .LastCommitShort }}</span></span>
          <span>newest-changed: <span class="mono">{{ .NewestChangedCommitShort }}</span></span>
        </div>

        <!-- Observations Table -->
        <div style="overflow-x:auto;margin-top:16px">
          <table class="ci">
            <thead>
              <tr>
                <th>Commit</th>
                <th>Time</th>
                <th>Model</th>
                <th>Samples</th>
                <th>Score (95% CI)</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {{ range .Observations }}
              <tr>
                <td class="mono">{{ .CommitShort }}</td>
                <td class="ts" title="{{ .TimeUTC }}">{{ .TimeStr }}</td>
                <td class="mono">{{ if .Model }}{{ .Model }}{{ else }}—{{ end }}</td>
                <td class="num">{{ .Samples }}</td>
                <td>
                  <span class="mono">{{ .ScoreStr }}</span>
                  <span style="font-size:12px;color:var(--muted);margin-left:4px">{{ .CIStr }}</span>
                </td>
                <td>
                  {{- if .SamplesLink }}
                  <a href="{{ .SamplesLink }}" class="mono" style="font-size:13px">view samples →</a>
                  {{- else }}
                  <span style="color:var(--muted)">—</span>
                  {{- end }}
                </td>
              </tr>
              {{ end }}
            </tbody>
          </table>
        </div>
      </div>
    </div>
    {{- end }}

    <!-- 4. Footer Note -->
    <div class="card" style="margin-top:24px">
      <div style="padding:14px 24px;font-size:12px;color:var(--muted);line-height:1.5">
        Score at main is the pooled Wilson success rate over the newest completed baseline observations at the current commit. An observation is one completed task result on one Run.
      </div>
    </div>
  </main>
</body>
</html>
`
