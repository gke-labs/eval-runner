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

package e2e

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/webhook"
)

// runWebhookSmoke verifies that the eval-runner-webhook service is up in kind,
// verifies probes (/healthz, /readyz), posts a signed recorded pull_request payload,
// and verifies that candidate Run, baseline Run, and Comparison are created via RBAC.
func runWebhookSmoke(t *testing.T, harness *Harness) {
	// Pick an available local port for port-forwarding
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on local port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()

	// Port forward to the Webhook service on local port
	cmd := exec.Command("kubectl", "port-forward", "-n", "eval-runner", "svc/eval-runner-webhook", fmt.Sprintf("%d:80", port))
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start port-forward: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
	}()

	// Wait for port-forward to be ready
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	ready := false
	for i := 0; i < 30; i++ {
		resp, err := http.Get(baseURL + "/healthz")
		if err == nil && resp.StatusCode == http.StatusOK {
			ready = true
			resp.Body.Close()
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !ready {
		t.Fatalf("eval-runner-webhook port-forward failed to become ready")
	}

	// 1. Curl /healthz
	resp, err := http.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatalf("failed to GET /healthz: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "ok\n" {
		t.Fatalf("unexpected /healthz response: status=%d body=%q", resp.StatusCode, string(body))
	}

	// 2. Curl /readyz
	respReady, err := http.Get(baseURL + "/readyz")
	if err != nil {
		t.Fatalf("failed to GET /readyz: %v", err)
	}
	bodyReady, _ := io.ReadAll(respReady.Body)
	respReady.Body.Close()
	if respReady.StatusCode != http.StatusOK || string(bodyReady) != "ok\n" {
		t.Fatalf("unexpected /readyz response: status=%d body=%q", respReady.StatusCode, string(bodyReady))
	}

	// 3. Load recorded pull_request.json fixture and sign with e2e-webhook-secret
	gitRoot := harness.GetGitRoot()
	payloadPath := filepath.Join(gitRoot, "pkg/webhook/testdata/pull_request.json")
	payload, err := os.ReadFile(payloadPath)
	if err != nil {
		t.Fatalf("failed to read recorded pull_request fixture at %s: %v", payloadPath, err)
	}

	webhookSecret := []byte("e2e-webhook-secret")
	signature := webhook.ComputeSignature(payload, webhookSecret)

	candRunName := "pr-305-aaaaaaa"
	baseRunName := "base-bbbbbbb"
	compName := "pr-305"

	t.Cleanup(func() {
		harness.runCmd("kubectl", "delete", "run", candRunName, "-n", "eval-runner", "--ignore-not-found")
		harness.runCmd("kubectl", "delete", "run", baseRunName, "-n", "eval-runner", "--ignore-not-found")
		harness.runCmd("kubectl", "delete", "comparison", compName, "-n", "eval-runner", "--ignore-not-found")
	})

	// 4. POST signed recorded webhook payload to /webhook
	req, err := http.NewRequest(http.MethodPost, baseURL+"/webhook", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("failed to create webhook HTTP request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(webhook.HeaderGitHubEvent, webhook.EventPullRequest)
	req.Header.Set(webhook.HeaderHubSignature256, signature)
	req.Header.Set(webhook.HeaderGitHubDelivery, "e2e-smoke-delivery")

	postResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to POST webhook payload: %v", err)
	}
	postBody, _ := io.ReadAll(postResp.Body)
	postResp.Body.Close()
	if postResp.StatusCode != http.StatusOK {
		t.Fatalf("webhook returned status %d: %s", postResp.StatusCode, string(postBody))
	}

	// 5. Assert Candidate Run, Baseline Run, and Comparison are created via RBAC
	candRun := harness.WaitForRunExists("eval-runner", candRunName, 30*time.Second)
	if candRun.Spec.Repo.URL != "https://github.com/contributor-fork/eval-runner.git" {
		t.Errorf("expected candidate repo URL https://github.com/contributor-fork/eval-runner.git, got %s", candRun.Spec.Repo.URL)
	}
	if candRun.Labels[webhook.LabelPR] != "305" {
		t.Errorf("expected candidate label %s=305, got %s", webhook.LabelPR, candRun.Labels[webhook.LabelPR])
	}
	if candRun.Spec.SampleRange == nil || candRun.Spec.SampleRange.Start != 1 || candRun.Spec.SampleRange.End != 8 {
		t.Errorf("expected candidate SampleRange {1, 8}, got %+v", candRun.Spec.SampleRange)
	}
	if candRun.Spec.Limit != nil {
		t.Errorf("expected candidate limit nil, got %v", candRun.Spec.Limit)
	}

	baseRun := harness.WaitForRunExists("eval-runner", baseRunName, 30*time.Second)
	if baseRun.Spec.Repo.URL != "https://github.com/gke-labs/eval-runner.git" {
		t.Errorf("expected baseline repo URL https://github.com/gke-labs/eval-runner.git, got %s", baseRun.Spec.Repo.URL)
	}
	if baseRun.Spec.SampleRange == nil || baseRun.Spec.SampleRange.Start != 1 || baseRun.Spec.SampleRange.End != 8 {
		t.Errorf("expected baseline SampleRange {1, 8}, got %+v", baseRun.Spec.SampleRange)
	}
	if baseRun.Spec.Limit != nil {
		t.Errorf("expected baseline limit nil, got %v", baseRun.Spec.Limit)
	}

	comp := harness.WaitForComparisonExists("eval-runner", compName, 30*time.Second)
	if comp.Spec.Candidate.Run != candRunName {
		t.Errorf("expected comparison candidate %s, got %s", candRunName, comp.Spec.Candidate.Run)
	}
	if comp.Spec.Baseline.Run != baseRunName {
		t.Errorf("expected comparison baseline %s, got %s", baseRunName, comp.Spec.Baseline.Run)
	}
	if comp.Spec.PullRequest == nil || comp.Spec.PullRequest.Number != 305 {
		t.Errorf("expected comparison PullRequest.Number=305, got %+v", comp.Spec.PullRequest)
	}
	if comp.Spec.Sampling == nil || comp.Spec.Sampling.BatchSize != 8 || comp.Spec.Sampling.MaxSamplesPerTask != 64 {
		t.Errorf("expected comparison sampling {8, 64}, got %+v", comp.Spec.Sampling)
	}

	// 6. Post recorded signed issue_comment payload and assert spec.extraSamples is bumped
	commentPayloadPath := filepath.Join(gitRoot, "pkg/webhook/testdata/issue_comment.json")
	commentPayload, err := os.ReadFile(commentPayloadPath)
	if err != nil {
		t.Fatalf("failed to read recorded issue_comment fixture at %s: %v", commentPayloadPath, err)
	}
	commentSig := webhook.ComputeSignature(commentPayload, webhookSecret)

	reqComment, err := http.NewRequest(http.MethodPost, baseURL+"/webhook", bytes.NewReader(commentPayload))
	if err != nil {
		t.Fatalf("failed to create comment webhook HTTP request: %v", err)
	}
	reqComment.Header.Set("Content-Type", "application/json")
	reqComment.Header.Set(webhook.HeaderGitHubEvent, webhook.EventIssueComment)
	reqComment.Header.Set(webhook.HeaderHubSignature256, commentSig)
	reqComment.Header.Set(webhook.HeaderGitHubDelivery, "e2e-smoke-comment-delivery")

	commentResp, err := http.DefaultClient.Do(reqComment)
	if err != nil {
		t.Fatalf("failed to POST comment webhook payload: %v", err)
	}
	commentRespBody, _ := io.ReadAll(commentResp.Body)
	commentResp.Body.Close()
	if commentResp.StatusCode != http.StatusOK {
		t.Fatalf("comment webhook returned status %d: %s", commentResp.StatusCode, string(commentRespBody))
	}

	// 7. Verify spec.extraSamples on Comparison is bumped to 8
	var updatedComp *v1alpha1.Comparison
	for i := 0; i < 30; i++ {
		updatedComp = harness.WaitForComparisonExists("eval-runner", compName, 5*time.Second)
		if updatedComp.Spec.ExtraSamples == 8 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if updatedComp.Spec.ExtraSamples != 8 {
		t.Errorf("expected comparison ExtraSamples=8, got %d", updatedComp.Spec.ExtraSamples)
	}

	// 8. Post signed pull_request closed event and assert Comparison and Candidate Run are deleted while Baseline Run is preserved
	closedPayload := []byte(`{
		"action": "closed",
		"number": 305,
		"pull_request": {
			"number": 305,
			"head": {
				"sha": "aaaaaaa111111122222223333333444444455555",
				"ref": "feature",
				"repo": {
					"full_name": "contributor-fork/eval-runner",
					"name": "eval-runner",
					"owner": {"login": "contributor-fork"}
				}
			},
			"base": {
				"sha": "bbbbbbb111111122222223333333444444455555",
				"ref": "main",
				"repo": {
					"full_name": "gke-labs/eval-runner",
					"name": "eval-runner",
					"owner": {"login": "gke-labs"}
				}
			}
		},
		"repository": {
			"full_name": "gke-labs/eval-runner",
			"name": "eval-runner",
			"owner": {"login": "gke-labs"}
		}
	}`)
	closedSig := webhook.ComputeSignature(closedPayload, webhookSecret)

	reqClosed, err := http.NewRequest(http.MethodPost, baseURL+"/webhook", bytes.NewReader(closedPayload))
	if err != nil {
		t.Fatalf("failed to create closed pull_request webhook HTTP request: %v", err)
	}
	reqClosed.Header.Set("Content-Type", "application/json")
	reqClosed.Header.Set(webhook.HeaderGitHubEvent, webhook.EventPullRequest)
	reqClosed.Header.Set(webhook.HeaderHubSignature256, closedSig)
	reqClosed.Header.Set(webhook.HeaderGitHubDelivery, "e2e-smoke-closed-delivery")

	closedResp, err := http.DefaultClient.Do(reqClosed)
	if err != nil {
		t.Fatalf("failed to POST closed pull_request webhook payload: %v", err)
	}
	closedRespBody, _ := io.ReadAll(closedResp.Body)
	closedResp.Body.Close()
	if closedResp.StatusCode != http.StatusOK {
		t.Fatalf("closed pull_request webhook returned status %d: %s", closedResp.StatusCode, string(closedRespBody))
	}

	// 9. Assert Comparison and Candidate Run are deleted; Baseline Run is preserved
	harness.WaitForComparisonDeleted("eval-runner", compName, 30*time.Second)
	harness.WaitForRunDeleted("eval-runner", candRunName, 30*time.Second)

	// Baseline Run must be preserved for future PR reuse
	_ = harness.WaitForRunExists("eval-runner", baseRunName, 5*time.Second)
}

// TestWebhookSmoke is retained for backwards compatibility with `go test -run TestWebhookSmoke`.
// When running the full E2E suite, the smoke test is run as a subtest of TestRunDiscovery
// (TestRunDiscovery/Webhook) to avoid redundant deploy overhead.
func TestWebhookSmoke(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("skipping e2e test: RUN_E2E is not set")
	}

	t.Skip("TestWebhookSmoke is run as a subtest of TestRunDiscovery (TestRunDiscovery/Webhook) to avoid redundant deploy overhead")
}
