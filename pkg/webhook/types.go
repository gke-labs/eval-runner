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

package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

const (
	// LabelPR tracks the pull request number on created Runs and Comparisons.
	LabelPR = "evalrunner.gke-labs.dev/pr"

	// LabelRepo tracks the repository identifier on created Runs and Comparisons.
	LabelRepo = "evalrunner.gke-labs.dev/repo"

	// LabelHeadSHA tracks the git commit SHA on created Runs and Comparisons.
	LabelHeadSHA = "evalrunner.gke-labs.dev/head-sha"

	// HeaderHubSignature256 is the GitHub webhook HMAC-SHA256 signature header.
	HeaderHubSignature256 = "X-Hub-Signature-256"

	// HeaderGitHubEvent is the GitHub webhook event type header.
	HeaderGitHubEvent = "X-GitHub-Event"

	// HeaderGitHubDelivery is the GitHub webhook delivery UUID header.
	HeaderGitHubDelivery = "X-GitHub-Delivery"

	// EventPullRequest is the GitHub event name for pull requests.
	EventPullRequest = "pull_request"

	// EventPush is the GitHub event name for git pushes.
	EventPush = "push"

	// EventIssueComment is the GitHub event name for issue comments.
	EventIssueComment = "issue_comment"

	// EventPing is the GitHub event name for webhook ping.
	EventPing = "ping"

	// ActionOpened is the pull_request action when a PR is opened.
	ActionOpened = "opened"

	// ActionSynchronize is the pull_request action when new commits are pushed to a PR.
	ActionSynchronize = "synchronize"

	// ActionReopened is the pull_request action when a closed PR is reopened.
	ActionReopened = "reopened"

	// ActionClosed is the pull_request action when a PR is closed or merged.
	ActionClosed = "closed"

	// ActionCreated is the issue_comment action when a comment is created.
	ActionCreated = "created"

	// AnnotationHandledComments is the Comparison annotation tracking handled issue comment IDs.
	AnnotationHandledComments = "evalrunner.gke-labs.dev/handled-comments"
)

var (
	repoPartRegex  = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
	commitSHARegex = regexp.MustCompile(`^[0-9a-f]{7,64}$`)
)

// GitHubRepository represents repository metadata in GitHub webhook payloads.
type GitHubRepository struct {
	Name          string `json:"name"`
	FullName      string `json:"full_name"` // "owner/name"
	CloneURL      string `json:"clone_url"`
	DefaultBranch string `json:"default_branch"`
	Owner         struct {
		Login string `json:"login"`
	} `json:"owner"`
}

// GitHubCommitRef represents a git commit reference (head or base) in a pull request.
type GitHubCommitRef struct {
	SHA  string            `json:"sha"`
	Ref  string            `json:"ref"`
	Repo *GitHubRepository `json:"repo"`
}

// GitHubPullRequestData represents pull request details in GitHub webhook payloads.
type GitHubPullRequestData struct {
	Number int             `json:"number"`
	State  string          `json:"state"`
	Head   GitHubCommitRef `json:"head"`
	Base   GitHubCommitRef `json:"base"`
}

// GitHubPullRequestEvent represents the payload of a GitHub pull_request webhook event.
type GitHubPullRequestEvent struct {
	Action      string                `json:"action"`
	Number      int                   `json:"number"`
	PullRequest GitHubPullRequestData `json:"pull_request"`
	Repository  GitHubRepository      `json:"repository"`
}

// GitHubPushEvent represents the payload of a GitHub push webhook event.
type GitHubPushEvent struct {
	Ref        string           `json:"ref"`    // e.g. "refs/heads/main"
	Before     string           `json:"before"` // previous commit SHA
	After      string           `json:"after"`  // new commit SHA
	Deleted    bool             `json:"deleted"`
	Repository GitHubRepository `json:"repository"`
	HeadCommit *struct {
		ID string `json:"id"`
	} `json:"head_commit"`
}

// GitHubIssueData represents the issue in an issue_comment event payload.
type GitHubIssueData struct {
	Number      int    `json:"number"`
	State       string `json:"state"`
	PullRequest *struct {
		URL     string `json:"url"`
		HTMLURL string `json:"html_url"`
	} `json:"pull_request"`
}

// GitHubCommentData represents the comment in an issue_comment event payload.
type GitHubCommentData struct {
	ID   int64  `json:"id"`
	Body string `json:"body"`
	User struct {
		Login string `json:"login"`
		Type  string `json:"type"`
	} `json:"user"`
}

// GitHubIssueCommentEvent represents the payload of a GitHub issue_comment webhook event.
type GitHubIssueCommentEvent struct {
	Action     string            `json:"action"`
	Issue      GitHubIssueData   `json:"issue"`
	Comment    GitHubCommentData `json:"comment"`
	Repository GitHubRepository  `json:"repository"`
	Sender     struct {
		Login string `json:"login"`
		Type  string `json:"type"`
	} `json:"sender"`
}

var evalMoreRegex = regexp.MustCompile(`(?im)^\s*/eval\s+more\s+(\d+)\s*$`)

// ParseEvalMoreCommand parses a comment body for an `/eval more <n>` command.
// If found, it returns the number of extra samples (bounded by maxSamplesPerTask if > 0)
// and true. If not found or if n <= 0, it returns 0 and false.
func ParseEvalMoreCommand(body string, maxSamplesPerTask int) (int, bool) {
	matches := evalMoreRegex.FindStringSubmatch(body)
	if len(matches) < 2 {
		return 0, false
	}
	n, err := strconv.Atoi(matches[1])
	if err != nil || n <= 0 {
		return 0, false
	}
	if maxSamplesPerTask > 0 && n > maxSamplesPerTask {
		n = maxSamplesPerTask
	}
	return n, true
}

// PullRequestWorkItem contains pre-validated fields for PR reconciliation in the worker.
type PullRequestWorkItem struct {
	Action            string
	PRNumber          int
	HeadSHA           string
	BaseSHA           string
	CandidateCloneURL string
	CandidateRepo     string // "owner/name"
	BaselineCloneURL  string
	BaselineRepo      string // "owner/name"
	TargetRepo        string // "owner/name" for pullRequest spec
}

// PushWorkItem contains pre-validated fields for push reconciliation in the worker.
type PushWorkItem struct {
	CommitSHA string
	CloneURL  string
	Repo      string // "owner/name"
}

// ValidateRepoComponent validates that an owner or repo name matches ^[A-Za-z0-9_.-]+$.
func ValidateRepoComponent(s string) error {
	if !repoPartRegex.MatchString(s) {
		return fmt.Errorf("invalid repository component %q: must match %s", s, repoPartRegex.String())
	}
	return nil
}

// ValidateCommitSHA validates that a commit SHA matches ^[0-9a-f]{7,64}$.
func ValidateCommitSHA(sha string) error {
	trimmed := strings.ToLower(strings.TrimSpace(sha))
	if !commitSHARegex.MatchString(trimmed) {
		return fmt.Errorf("invalid commit SHA %q: must match %s", sha, commitSHARegex.String())
	}
	return nil
}

// BuildCloneURL returns the HTTPS git clone URL for a given owner and repository name.
func BuildCloneURL(owner, name string) string {
	return fmt.Sprintf("https://github.com/%s/%s.git", owner, name)
}

// ParseOwnerAndName extracts and validates the repository owner and name from a GitHubRepository.
func ParseOwnerAndName(repo *GitHubRepository) (string, string, error) {
	if repo == nil {
		return "", "", errors.New("repository object is nil")
	}
	owner := strings.TrimSpace(repo.Owner.Login)
	name := strings.TrimSpace(repo.Name)
	if (owner == "" || name == "") && strings.Contains(repo.FullName, "/") {
		parts := strings.SplitN(repo.FullName, "/", 2)
		if owner == "" {
			owner = strings.TrimSpace(parts[0])
		}
		if name == "" {
			name = strings.TrimSpace(parts[1])
		}
	}
	if owner == "" || name == "" {
		return "", "", fmt.Errorf("missing repository owner or name (owner=%q, name=%q, full_name=%q)", owner, name, repo.FullName)
	}
	if err := ValidateRepoComponent(owner); err != nil {
		return "", "", fmt.Errorf("invalid owner: %w", err)
	}
	if err := ValidateRepoComponent(name); err != nil {
		return "", "", fmt.Errorf("invalid repository name: %w", err)
	}
	return owner, name, nil
}

// ShortSHA returns the first 7 characters of a git commit SHA in lowercase.
// If the SHA is shorter than 7 characters, it is returned trimmed and in lowercase.
func ShortSHA(sha string) string {
	s := strings.ToLower(strings.TrimSpace(sha))
	if len(s) <= 7 {
		return s
	}
	return s[:7]
}

// SanitizeLabelValue converts an arbitrary string (such as "owner/name") into a valid
// Kubernetes label value:
// - Max 63 characters
// - Characters must be [a-zA-Z0-9_.-]
// - Must start and end with an alphanumeric character ([a-zA-Z0-9])
func SanitizeLabelValue(val string) string {
	val = strings.TrimSpace(val)
	if val == "" {
		return ""
	}

	var sb strings.Builder
	for _, r := range val {
		switch {
		case r == '/':
			sb.WriteRune('_')
		case unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '.' || r == '-':
			sb.WriteRune(r)
		default:
			sb.WriteRune('_')
		}
	}

	s := sb.String()
	// Trim non-alphanumeric from edges
	s = strings.TrimFunc(s, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})

	if len(s) > 63 {
		s = s[:63]
		s = strings.TrimFunc(s, func(r rune) bool {
			return !unicode.IsLetter(r) && !unicode.IsDigit(r)
		})
	}
	return s
}

// ComputeSignature computes the GitHub X-Hub-Signature-256 HMAC-SHA256 header value
// formatted as "sha256=<hex>".
func ComputeSignature(payload, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(payload)
	return fmt.Sprintf("sha256=%s", hex.EncodeToString(mac.Sum(nil)))
}

// VerifySignature verifies that the payload matches the X-Hub-Signature-256 header
// using HMAC-SHA256 and constant-time comparison.
func VerifySignature(payload, secret []byte, signatureHeader string) bool {
	if len(secret) == 0 || signatureHeader == "" {
		return false
	}
	const prefix = "sha256="
	if !strings.HasPrefix(signatureHeader, prefix) {
		return false
	}
	hexSig := strings.TrimPrefix(signatureHeader, prefix)
	expectedMAC, err := hex.DecodeString(hexSig)
	if err != nil {
		return false
	}

	mac := hmac.New(sha256.New, secret)
	mac.Write(payload)
	actualMAC := mac.Sum(nil)

	return hmac.Equal(actualMAC, expectedMAC)
}
