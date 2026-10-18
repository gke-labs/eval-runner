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

package github

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
)

func sameCommitComparisonFixture() (*v1alpha1.Comparison, *v1alpha1.Run, *v1alpha1.Run) {
	pImp := 0.26
	rate1 := 1.00
	rate2Cand := 0.15
	rate2Base := 0.20
	delta1 := 0.00
	delta2 := -0.05

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs-eval-same-commit",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline: v1alpha1.ComparisonRunRef{
				Run: "docs-eval-base-05ce4d8",
			},
			Candidate: v1alpha1.ComparisonRunRef{
				Run: "docs-eval-cand-05ce4d8",
			},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     98,
				HeadSHA:    "05ce4d8222222222222222222222222222222222",
				BaseSHA:    "05ce4d8111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:        v1alpha1.ComparisonPhaseCompleted,
			Verdict:      v1alpha1.ComparisonVerdictInconclusive,
			PImprovement: &pImp,
			SummaryPath:  "gs://eval-bucket/comparisons/docs-eval-same-commit.json",
			Summary: &v1alpha1.ComparisonSummary{
				Tasks: "2/2",
			},
			Tasks: []v1alpha1.ComparisonTaskStatus{
				{
					ID:            "docs/readme_comprehension",
					BaseRate:      &rate1,
					CandidateRate: &rate1,
					Delta:         &delta1,
					DeltaCI: &v1alpha1.DeltaConfidenceInterval{
						Lower: 0.00,
						Upper: 0.00,
					},
					Label:  v1alpha1.ComparisonTaskLabelUnchanged,
					Pairs:  20,
					Wins:   0,
					Losses: 0,
					Ties:   20,
				},
				{
					ID:            "docs/readme_comprehension_blind",
					BaseRate:      &rate2Base,
					CandidateRate: &rate2Cand,
					Delta:         &delta2,
					DeltaCI: &v1alpha1.DeltaConfidenceInterval{
						Lower: -0.15,
						Upper: 0.00,
					},
					Label:  v1alpha1.ComparisonTaskLabelInconclusive,
					Pairs:  20,
					Wins:   0,
					Losses: 1,
					Ties:   19,
				},
			},
		},
	}

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs-eval-base-05ce4d8",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				Commit: "05ce4d8111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.RunStatus{
			Summary: &v1alpha1.RunSummary{
				TotalSamples: 40,
			},
		},
	}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs-eval-cand-05ce4d8",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				Commit: "05ce4d8222222222222222222222222222222222",
			},
		},
		Status: v1alpha1.RunStatus{
			Summary: &v1alpha1.RunSummary{
				TotalSamples: 40,
			},
		},
	}
	return comp, baseRun, candRun
}

func TestRenderComparisonMarkdown_Golden(t *testing.T) {
	comp, baseRun, candRun := sameCommitComparisonFixture()
	got := RenderComparisonMarkdown(comp, baseRun, candRun, "")

	goldenPath := "testdata/same_commit_comparison.golden.md"
	wantBytes, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("failed to read golden file: %v", err)
	}
	want := string(wantBytes)

	if got != want {
		t.Errorf("RenderComparisonMarkdown mismatch (-got +want):\nGOT:\n%s\nWANT:\n%s", got, want)
	}
}

func TestCheckRunStateForComparison_Matrix(t *testing.T) {
	tests := []struct {
		name               string
		verdict            v1alpha1.ComparisonVerdict
		pImp               *float64
		candSamples        int
		candLimit          *int
		candTasks          []v1alpha1.TaskStatus
		baseSamples        int
		baseLimit          *int
		baseTasks          []v1alpha1.TaskStatus
		expectedStatus     string
		expectedConclusion string
		expectedTitle      string
	}{
		{
			name:    "RunningWithSamples",
			verdict: v1alpha1.ComparisonVerdictRunning,
			candTasks: []v1alpha1.TaskStatus{
				{ID: "task1"},
			},
			candLimit:   ptr(48),
			candSamples: 24,
			baseTasks: []v1alpha1.TaskStatus{
				{ID: "task1"},
			},
			baseLimit:          ptr(48),
			baseSamples:        0,
			expectedStatus:     "in_progress",
			expectedConclusion: "",
			expectedTitle:      "running · 24 of 96 samples",
		},
		{
			name:               "RunningNoSamplesKnown",
			verdict:            v1alpha1.ComparisonVerdictRunning,
			expectedStatus:     "in_progress",
			expectedConclusion: "",
			expectedTitle:      "running",
		},
		{
			name:               "ImprovedWithP",
			verdict:            v1alpha1.ComparisonVerdictImproved,
			pImp:               ptr(0.98),
			expectedStatus:     "completed",
			expectedConclusion: "success",
			expectedTitle:      "IMPROVED · P(improvement) = 0.98",
		},
		{
			name:               "Unchanged",
			verdict:            v1alpha1.ComparisonVerdictUnchanged,
			pImp:               ptr(0.50),
			expectedStatus:     "completed",
			expectedConclusion: "success",
			expectedTitle:      "UNCHANGED",
		},
		{
			name:               "Inconclusive",
			verdict:            v1alpha1.ComparisonVerdictInconclusive,
			pImp:               ptr(0.26),
			expectedStatus:     "completed",
			expectedConclusion: "neutral",
			expectedTitle:      "INCONCLUSIVE · more samples recommended",
		},
		{
			name:               "Regressed",
			verdict:            v1alpha1.ComparisonVerdictRegressed,
			pImp:               ptr(0.04),
			expectedStatus:     "completed",
			expectedConclusion: "failure",
			expectedTitle:      "REGRESSED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			comp := &v1alpha1.Comparison{
				Status: v1alpha1.ComparisonStatus{
					Verdict:      tt.verdict,
					PImprovement: tt.pImp,
				},
			}
			var candRun, baseRun *v1alpha1.Run
			if tt.candSamples > 0 || len(tt.candTasks) > 0 || tt.candLimit != nil {
				candRun = &v1alpha1.Run{
					Spec: v1alpha1.RunSpec{
						Limit: tt.candLimit,
					},
					Status: v1alpha1.RunStatus{
						Summary: &v1alpha1.RunSummary{TotalSamples: tt.candSamples},
						Tasks:   tt.candTasks,
					},
				}
			}
			if tt.baseSamples > 0 || len(tt.baseTasks) > 0 || tt.baseLimit != nil {
				baseRun = &v1alpha1.Run{
					Spec: v1alpha1.RunSpec{
						Limit: tt.baseLimit,
					},
					Status: v1alpha1.RunStatus{
						Summary: &v1alpha1.RunSummary{TotalSamples: tt.baseSamples},
						Tasks:   tt.baseTasks,
					},
				}
			}

			state := CheckRunStateForComparison(comp, baseRun, candRun)
			if state.Name != CheckRunName {
				t.Errorf("Name = %q, want %q", state.Name, CheckRunName)
			}
			if state.Status != tt.expectedStatus {
				t.Errorf("Status = %q, want %q", state.Status, tt.expectedStatus)
			}
			if state.Conclusion != tt.expectedConclusion {
				t.Errorf("Conclusion = %q, want %q", state.Conclusion, tt.expectedConclusion)
			}
			if state.Title != tt.expectedTitle {
				t.Errorf("Title = %q, want %q", state.Title, tt.expectedTitle)
			}
		})
	}
}

func TestReporter_LifecycleAndErrors(t *testing.T) {
	ctx := t.Context()
	server := NewFakeGitHubServer()
	defer server.Close()

	client := server.Client("test-token")
	reporter := NewReporter(client, ReporterOptions{})

	comp, baseRun, candRun := sameCommitComparisonFixture()

	// 1. First post creates comment and check run
	status1, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("first Report failed: %v", err)
	}
	if status1.CommentID == 0 {
		t.Errorf("expected non-zero CommentID, got %d", status1.CommentID)
	}
	if status1.CheckRunID == 0 {
		t.Errorf("expected non-zero CheckRunID, got %d", status1.CheckRunID)
	}
	if status1.LastPostedHash == "" {
		t.Errorf("expected non-empty LastPostedHash")
	}

	comments := server.Comments()
	if len(comments) != 1 {
		t.Fatalf("expected 1 comment, got %d", len(comments))
	}
	if !HasComparisonMarker(comments[0].Body, comp.Namespace, comp.Name) {
		t.Errorf("expected comment to contain marker")
	}

	checkRuns := server.CheckRuns()
	if len(checkRuns) != 1 {
		t.Fatalf("expected 1 check run, got %d", len(checkRuns))
	}
	if checkRuns[0].Conclusion != "neutral" {
		t.Errorf("expected conclusion 'neutral', got %q", checkRuns[0].Conclusion)
	}

	// 2. Idempotency: second call with same status makes 0 HTTP calls
	reqCountBefore := len(server.RequestLog())
	comp.Status.Reporting = status1
	status2, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("idempotent Report failed: %v", err)
	}
	if status2.LastPostedHash != status1.LastPostedHash {
		t.Errorf("expected same hash, got %s vs %s", status2.LastPostedHash, status1.LastPostedHash)
	}
	if len(server.RequestLog()) != reqCountBefore {
		t.Errorf("expected 0 HTTP requests on idempotent Report, got %d new requests", len(server.RequestLog())-reqCountBefore)
	}

	// 3. Status change updates existing comment and check run
	pImpImproved := 0.98
	comp.Status.Verdict = v1alpha1.ComparisonVerdictImproved
	comp.Status.PImprovement = &pImpImproved

	status3, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("updating Report failed: %v", err)
	}
	if status3.CommentID != status1.CommentID {
		t.Errorf("expected same CommentID %d, got %d", status1.CommentID, status3.CommentID)
	}
	if status3.CheckRunID != status1.CheckRunID {
		t.Errorf("expected same CheckRunID %d, got %d", status1.CheckRunID, status3.CheckRunID)
	}
	if status3.LastPostedHash == status1.LastPostedHash {
		t.Errorf("expected new LastPostedHash after verdict change")
	}

	commentsAfter := server.Comments()
	if len(commentsAfter) != 1 {
		t.Errorf("expected still only 1 comment (updated in-place), got %d", len(commentsAfter))
	}
	if !strings.Contains(commentsAfter[0].Body, "IMPROVED · P(improvement) = 0.98") {
		t.Errorf("expected updated comment body to reflect IMPROVED verdict")
	}

	checkRunsAfter := server.CheckRuns()
	if len(checkRunsAfter) != 1 {
		t.Errorf("expected still only 1 check run (updated in-place), got %d", len(checkRunsAfter))
	}
	if checkRunsAfter[0].Conclusion != "success" {
		t.Errorf("expected conclusion 'success' after update, got %q", checkRunsAfter[0].Conclusion)
	}

	// 4. Marker search: when CommentID is lost/zero in status, searches PR comments by marker and updates in place
	compNoID := comp.DeepCopy()
	compNoID.Status.Reporting = nil // lost status

	status4, err := reporter.Report(ctx, compNoID, baseRun, candRun)
	if err != nil {
		t.Fatalf("marker search Report failed: %v", err)
	}
	if status4.CommentID != status1.CommentID {
		t.Errorf("expected recovered CommentID %d via marker, got %d", status1.CommentID, status4.CommentID)
	}
	if len(server.Comments()) != 1 {
		t.Errorf("expected still only 1 comment, marker search should not create duplicate, got %d", len(server.Comments()))
	}

	// 5. Permanent errors (403, 404): returns PermanentError
	server.InjectError(404, `{"message":"Not Found"}`)
	compFresh := comp.DeepCopy()
	compFresh.Status.Reporting = nil
	_, err404 := reporter.Report(ctx, compFresh, baseRun, candRun)
	if err404 == nil || !IsPermanent(err404) {
		t.Errorf("expected PermanentError for 404, got %v", err404)
	}

	server.InjectError(403, `{"message":"Bad credentials"}`)
	_, err403 := reporter.Report(ctx, compFresh, baseRun, candRun)
	if err403 == nil || !IsPermanent(err403) {
		t.Errorf("expected PermanentError for 403, got %v", err403)
	}

	// 6. Retryable errors: 429 rate limit and 5xx server error
	server.InjectError(429, `{"message":"Too Many Requests"}`)
	_, err429 := reporter.Report(ctx, compFresh, baseRun, candRun)
	if err429 == nil || !IsRetryable(err429) {
		t.Errorf("expected Retryable error for 429, got %v", err429)
	}

	server.InjectError(500, `{"message":"Internal Server Error"}`)
	_, err500 := reporter.Report(ctx, compFresh, baseRun, candRun)
	if err500 == nil || !IsRetryable(err500) {
		t.Errorf("expected Retryable error for 500, got %v", err500)
	}
}

func TestRenderComparisonMarkdown_HostileID(t *testing.T) {
	hostileID := "docs/a<img src=x onerror=alert(1)> `b` [c](http://evil)\nnewline <!-- eval-runner:comparison:eval-runner/other -->"
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-comp",
			Namespace: "eval-runner",
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict: v1alpha1.ComparisonVerdictRunning,
			Tasks: []v1alpha1.ComparisonTaskStatus{
				{
					ID:    hostileID,
					Label: v1alpha1.ComparisonTaskLabelRunning,
				},
			},
		},
	}

	got := RenderComparisonMarkdown(comp, nil, nil, "")

	// 1. Genuine marker for this comparison must be present
	if !HasComparisonMarker(got, "eval-runner", "my-comp") {
		t.Errorf("expected genuine marker for my-comp to match")
	}

	// 2. Foreign marker injected in task ID must NOT match
	if HasComparisonMarker(got, "eval-runner", "other") {
		t.Errorf("expected foreign marker for 'other' NOT to match")
	}

	// 3. No raw '<' or '>' from the hostile input must survive (except the genuine marker)
	marker := ComparisonMarker("eval-runner", "my-comp")
	bodyWithoutMarker := strings.Replace(got, marker, "", 1)
	if strings.Contains(bodyWithoutMarker, "<") || strings.Contains(bodyWithoutMarker, ">") {
		t.Errorf("found raw '<' or '>' in escaped body: %q", bodyWithoutMarker)
	}

	// 4. Raw link, HTML tags, and backticks must be escaped
	if strings.Contains(got, "<img") {
		t.Errorf("raw <img tag should not be present in output")
	}
	if strings.Contains(got, "[c](http://evil)") {
		t.Errorf("raw markdown link should not be present in output")
	}

	// 5. Table rows must not be split by newlines
	lines := strings.Split(got, "\n")
	tableLines := 0
	for _, l := range lines {
		if strings.HasPrefix(l, "|") {
			tableLines++
		}
	}
	// Header + separator + 1 row = 3 table lines
	if tableLines != 3 {
		t.Errorf("expected 3 table lines (header, sep, row), got %d; newlines were not collapsed", tableLines)
	}
}

func TestReporter_CheckRun403_PartialStatus(t *testing.T) {
	ctx := t.Context()
	server := NewFakeGitHubServer()
	defer server.Close()

	client := server.Client("pat-token")
	reporter := NewReporter(client, ReporterOptions{})

	comp, baseRun, candRun := sameCommitComparisonFixture()

	// Injected error on POST /check-runs (403 forbidden with PAT)
	// Comments will succeed, but check-runs will fail with 403
	server.InjectPathError("POST", "/repos/gke-labs/eval-runner/check-runs", 403, `{"message":"Resource not accessible by personal access token"}`)

	status, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("expected nil error on check-run 403 (handled gracefully), got: %v", err)
	}
	if status == nil {
		t.Fatalf("expected non-nil partial status")
	}
	if status.CommentID == 0 {
		t.Errorf("expected commentID to be preserved in partial status")
	}
	if status.LastPostedHash == "" {
		t.Errorf("expected lastPostedHash to be preserved in partial status")
	}
	if status.CheckRunID != 0 {
		t.Errorf("expected checkRunID 0, got %d", status.CheckRunID)
	}
	if status.Message != "check runs unavailable with token auth; comment posted" {
		t.Errorf("expected message 'check runs unavailable with token auth; comment posted', got %q", status.Message)
	}

	// Verify comment was created on server
	if len(server.Comments()) != 1 {
		t.Errorf("expected 1 comment created, got %d", len(server.Comments()))
	}

	// Verify idempotency: subsequent reconcile with same status makes 0 requests (gated on hash)
	reqCountBefore := len(server.RequestLog())
	comp.Status.Reporting = status
	status2, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("idempotent report failed: %v", err)
	}
	if status2.LastPostedHash != status.LastPostedHash {
		t.Errorf("expected same hash")
	}
	if len(server.RequestLog()) != reqCountBefore {
		t.Errorf("expected 0 new requests on idempotent call, got %d", len(server.RequestLog())-reqCountBefore)
	}
}

func TestAppAuthentication(t *testing.T) {
	ctx := t.Context()
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate test RSA key: %v", err)
	}

	pkcs1PEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privKey),
	})

	pkcs8Bytes, err := x509.MarshalPKCS8PrivateKey(privKey)
	if err != nil {
		t.Fatalf("failed to marshal PKCS8 key: %v", err)
	}
	pkcs8PEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: pkcs8Bytes,
	})

	// 1. Verify key parsing for PKCS#1 and PKCS#8
	if _, err := ParseRSAPrivateKey(pkcs1PEM); err != nil {
		t.Errorf("failed to parse PKCS#1 private key: %v", err)
	}
	if _, err := ParseRSAPrivateKey(pkcs8PEM); err != nil {
		t.Errorf("failed to parse PKCS#8 private key: %v", err)
	}
	if _, err := ParseRSAPrivateKey([]byte("not a pem key")); err == nil {
		t.Errorf("expected error parsing invalid PEM key")
	}

	// 2. Verify JWT generation
	jwt, err := GenerateAppJWT("123456", privKey, time.Now())
	if err != nil {
		t.Fatalf("failed to generate app JWT: %v", err)
	}
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts in JWT, got %d", len(parts))
	}

	// 3. Test AppTokenProvider with fake server
	server := NewFakeGitHubServer()
	defer server.Close()

	server.SetAppSlug("custom-eval-app")

	provider, err := NewAppTokenProvider(AppConfig{
		AppID:          "123456",
		InstallationID: "987654",
		PrivateKeyPEM:  pkcs1PEM,
		BaseURL:        server.URL(),
		HTTPClient:     server.server.Client(),
	})
	if err != nil {
		t.Fatalf("failed to create AppTokenProvider: %v", err)
	}

	// First call exchanges JWT for token
	tok1, err := provider.Token(ctx)
	if err != nil {
		t.Fatalf("Token failed: %v", err)
	}
	if tok1 != "ghs_fake_installation_token" {
		t.Errorf("expected 'ghs_fake_installation_token', got %q", tok1)
	}

	// Second call reuses cached token
	tok2, err := provider.Token(ctx)
	if err != nil {
		t.Fatalf("Token second call failed: %v", err)
	}
	if tok2 != tok1 {
		t.Errorf("expected cached token %q, got %q", tok1, tok2)
	}

	// Test Identity fetches app slug
	identity, err := provider.Identity(ctx)
	if err != nil {
		t.Fatalf("Identity failed: %v", err)
	}
	if identity != "custom-eval-app[bot]" {
		t.Errorf("expected 'custom-eval-app[bot]', got %q", identity)
	}

	// 4. Test Report with AppClient creates check run (not 403)
	appClient, err := server.AppClient("123456", "987654", pkcs1PEM)
	if err != nil {
		t.Fatalf("failed to create AppClient: %v", err)
	}
	reporter := NewReporter(appClient, ReporterOptions{})
	comp, baseRun, candRun := sameCommitComparisonFixture()

	status, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("Report with app auth failed: %v", err)
	}
	if status.CommentID == 0 {
		t.Errorf("expected non-zero CommentID")
	}
	if status.CheckRunID == 0 {
		t.Errorf("expected non-zero CheckRunID with App auth")
	}
	if status.Message != "" {
		t.Errorf("expected empty message on success, got %q", status.Message)
	}
}

func TestListIssueComments_Pagination(t *testing.T) {
	ctx := t.Context()
	server := NewFakeGitHubServer()
	defer server.Close()

	// Create 5 comments on issue 42
	for i := 1; i <= 5; i++ {
		server.AddCommentWithAuthor("gke-labs", "eval-runner", 42, fmt.Sprintf("comment %d", i), "user1")
	}

	client := server.Client("test-token")
	comments, err := client.ListIssueComments(ctx, "gke-labs", "eval-runner", 42)
	if err != nil {
		t.Fatalf("ListIssueComments failed: %v", err)
	}
	if len(comments) != 5 {
		t.Fatalf("expected 5 comments across pages, got %d", len(comments))
	}
	for i, c := range comments {
		expectedBody := fmt.Sprintf("comment %d", i+1)
		if c.Body != expectedBody {
			t.Errorf("comment %d body = %q, want %q", i, c.Body, expectedBody)
		}
	}

	// Now add a 6th comment that contains our comparison marker
	markerBody := ComparisonMarker("eval-runner", "docs-eval-same-commit") + "\n\nExisting report"
	server.AddCommentWithAuthor("gke-labs", "eval-runner", 42, markerBody, "test-user")

	comp, baseRun, candRun := sameCommitComparisonFixture()
	comp.Spec.PullRequest.Number = 42

	reporter := NewReporter(client, ReporterOptions{})
	status, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("Report failed: %v", err)
	}
	// The reporter should find the comment on later pages rather than creating a duplicate
	if status.CommentID == 0 {
		t.Errorf("expected to find existing comment ID from pagination")
	}
	// Check total comments on server is still 6 (no new comment added)
	if len(server.Comments()) != 6 {
		t.Errorf("expected 6 comments on server (marker found, updated in place), got %d", len(server.Comments()))
	}
}

func TestReporter_AuthorCheck(t *testing.T) {
	ctx := t.Context()
	server := NewFakeGitHubServer()
	defer server.Close()

	server.SetCurrentUser("eval-runner[bot]")

	comp, baseRun, candRun := sameCommitComparisonFixture()
	comp.Spec.PullRequest.Number = 10

	// 1. Another user (e.g. malicious-user) creates a comment containing the comparison marker
	marker := ComparisonMarker(comp.Namespace, comp.Name)
	foreignCommentID := server.AddCommentWithAuthor("gke-labs", "eval-runner", 10, marker+"\nForeign hijack comment", "malicious-user")

	client := server.Client("bot-token")
	reporter := NewReporter(client, ReporterOptions{})

	// Report must NOT match malicious-user's comment and must create its own comment
	status, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("Report failed: %v", err)
	}
	if status.CommentID == foreignCommentID {
		t.Errorf("reporter hijacked foreign comment authored by malicious-user")
	}

	// Verify that the malicious comment is unchanged
	for _, c := range server.Comments() {
		if c.ID == foreignCommentID {
			if !strings.Contains(c.Body, "Foreign hijack comment") {
				t.Errorf("foreign comment was overwritten: %q", c.Body)
			}
		}
	}

	// 2. A subsequent report matches our own comment and updates it
	comp.Status.Reporting = nil // reset to test marker search with our own comment now present
	status2, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("second Report failed: %v", err)
	}
	if status2.CommentID != status.CommentID {
		t.Errorf("expected to match our own comment ID %d, got %d", status.CommentID, status2.CommentID)
	}
}

func TestRenderComparisonMarkdown_UIBaseURL(t *testing.T) {
	comp, baseRun, candRun := sameCommitComparisonFixture()

	// 1. Without uiBaseURL: link is rendered as code text
	gotDefault := RenderComparisonMarkdown(comp, baseRun, candRun, "")
	if !strings.Contains(gotDefault, "`/comparisons/eval-runner/docs-eval-same-commit`") {
		t.Errorf("expected code-text path when uiBaseURL is empty, got:\n%s", gotDefault)
	}
	if strings.Contains(gotDefault, "[web view]") {
		t.Errorf("unexpected '[web view]' when uiBaseURL is empty")
	}

	// 2. With uiBaseURL: link is rendered as absolute markdown link
	uiURL := "https://eval.example.com"
	gotURL := RenderComparisonMarkdown(comp, baseRun, candRun, uiURL)
	expectedLink := "[web view](https://eval.example.com/comparisons/eval-runner/docs-eval-same-commit)"
	if !strings.Contains(gotURL, expectedLink) {
		t.Errorf("expected %q when uiBaseURL is set, got:\n%s", expectedLink, gotURL)
	}

	// 3. Trailing slash is normalized
	gotTrailing := RenderComparisonMarkdown(comp, baseRun, candRun, "https://eval.example.com/")
	if !strings.Contains(gotTrailing, expectedLink) {
		t.Errorf("expected trailing slash to be normalized to %q, got:\n%s", expectedLink, gotTrailing)
	}

	// 4. Reporter passes UIBaseURL through to markdown
	server := NewFakeGitHubServer()
	defer server.Close()
	client := server.Client("token")
	reporter := NewReporter(client, ReporterOptions{
		UIBaseURL: "https://eval.example.com",
	})

	status, err := reporter.Report(t.Context(), comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("Report failed: %v", err)
	}
	comments := server.Comments()
	if len(comments) != 1 {
		t.Fatalf("expected 1 comment, got %d", len(comments))
	}
	if !strings.Contains(comments[0].Body, expectedLink) {
		t.Errorf("expected comment on server to contain %q, got:\n%s", expectedLink, comments[0].Body)
	}
	_ = status
}

func TestRenderComparisonMarkdown_SamplesRun(t *testing.T) {
	comp, baseRun, candRun := sameCommitComparisonFixture()
	// Set explicit SamplesRun on the tasks
	comp.Status.Tasks[0].SamplesRun = 32
	comp.Status.Tasks[1].SamplesRun = 16

	got := RenderComparisonMarkdown(comp, baseRun, candRun, "")
	if !strings.Contains(got, "| docs/readme\\_comprehension | 1.00 | 1.00 | +0.00 [+0.00, +0.00] | unchanged | 32 |") {
		t.Errorf("expected markdown to display 32 for task 0 runs, got:\n%s", got)
	}
	if !strings.Contains(got, "| docs/readme\\_comprehension\\_blind | 0.20 | 0.15 | -0.05 [-0.15, +0.00] | inconclusive | 16 |") {
		t.Errorf("expected markdown to display 16 for task 1 runs, got:\n%s", got)
	}
}

func TestReporter_CheckRunRetryableError_HashGateNotSwallowed(t *testing.T) {
	ctx := t.Context()
	server := NewFakeGitHubServer()
	defer server.Close()

	client := server.Client("token")
	reporter := NewReporter(client, ReporterOptions{})

	comp, baseRun, candRun := sameCommitComparisonFixture()

	// Inject 502 Bad Gateway on check-runs
	server.InjectPathError("POST", "/repos/gke-labs/eval-runner/check-runs", http.StatusBadGateway, `{"message":"Server Error"}`)

	// First call: comment succeeds, check-run fails with 502
	status1, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err == nil || !IsRetryable(err) {
		t.Fatalf("expected retryable error on 502, got %v", err)
	}
	if status1 == nil {
		t.Fatalf("expected non-nil partial status")
	}
	if status1.CommentID == 0 {
		t.Errorf("expected commentID to be preserved on retryable check-run failure")
	}
	if status1.LastPostedHash != "" {
		t.Errorf("expected LastPostedHash to be empty on retryable check-run failure, got %q", status1.LastPostedHash)
	}
	if status1.CheckRunID != 0 {
		t.Errorf("expected checkRunID 0, got %d", status1.CheckRunID)
	}

	// Simulate controller persisting partial status on reconcile requeue
	comp.Status.Reporting = status1

	// Second call: check-run error is gone; Report must NOT be swallowed by hash gate,
	// and must retry CreateCheckRun!
	checkRunsBefore := len(server.CheckRuns())
	status2, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}
	if status2.CheckRunID == 0 {
		t.Errorf("expected CheckRunID to be populated on retry")
	}
	if status2.LastPostedHash == "" {
		t.Errorf("expected LastPostedHash to be populated after successful check-run")
	}
	if len(server.CheckRuns()) != checkRunsBefore+1 {
		t.Errorf("expected CreateCheckRun to be executed on retry")
	}

	// Third call: content unchanged and CheckRun succeeded -> hash gate blocks duplicate requests
	reqCountBefore := len(server.RequestLog())
	comp.Status.Reporting = status2
	status3, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("third call failed: %v", err)
	}
	if status3.LastPostedHash != status2.LastPostedHash {
		t.Errorf("expected same hash")
	}
	if len(server.RequestLog()) != reqCountBefore {
		t.Errorf("expected 0 new requests on idempotent call, got %d", len(server.RequestLog())-reqCountBefore)
	}
}

func TestAppAuthentication_NoSlugError(t *testing.T) {
	ctx := t.Context()
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate test RSA key: %v", err)
	}

	pkcs1PEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privKey),
	})

	server := NewFakeGitHubServer()
	defer server.Close()
	server.SetAppSlug("") // empty slug

	provider, err := NewAppTokenProvider(AppConfig{
		AppID:          "123456",
		InstallationID: "987654",
		PrivateKeyPEM:  pkcs1PEM,
		BaseURL:        server.URL(),
		HTTPClient:     server.server.Client(),
	})
	if err != nil {
		t.Fatalf("failed to create AppTokenProvider: %v", err)
	}

	_, err = provider.Identity(ctx)
	if err == nil {
		t.Fatalf("expected error when /app returns empty slug, got nil")
	}
	if !IsPermanent(err) {
		t.Errorf("expected PermanentError when /app returns empty slug, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "did not contain an app slug") {
		t.Errorf("expected error message mentioning 'did not contain an app slug', got: %v", err)
	}
}

func TestListIssueComments_RejectsExternalLink(t *testing.T) {
	ctx := t.Context()
	server := NewFakeGitHubServer()
	defer server.Close()

	// Inject external Link header pointing outside server.URL
	server.InjectPathError("GET", "/repos/gke-labs/eval-runner/issues/1/comments", http.StatusOK, `[]`, map[string]string{
		"Link": `<https://evil.example.com/comments?page=2>; rel="next"`,
	})

	client := server.Client("test-token")
	_, err := client.ListIssueComments(ctx, "gke-labs", "eval-runner", 1)
	if err == nil {
		t.Fatalf("expected error following external link, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to follow link header outside base URL") {
		t.Errorf("expected error message to mention 'refusing to follow link header outside base URL', got: %v", err)
	}
}

func ptr[T any](v T) *T {
	return &v
}
