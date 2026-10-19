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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// IssueCommentUser represents the author of a GitHub issue comment.
type IssueCommentUser struct {
	Login string `json:"login"`
}

// IssueComment represents a GitHub pull request / issue comment.
type IssueComment struct {
	ID   int64            `json:"id"`
	Body string           `json:"body"`
	User IssueCommentUser `json:"user"`
}

// Client defines the interface for interacting with GitHub's PR comments and Check Runs.
type Client interface {
	CurrentUser(ctx context.Context) (string, error)
	ListIssueComments(ctx context.Context, owner, repo string, number int) ([]IssueComment, error)
	CreateIssueComment(ctx context.Context, owner, repo string, number int, body string) (int64, error)
	UpdateIssueComment(ctx context.Context, owner, repo string, commentID int64, body string) error
	CreateCheckRun(ctx context.Context, owner, repo string, headSHA string, state CheckRunState) (int64, error)
	UpdateCheckRun(ctx context.Context, owner, repo string, checkRunID int64, state CheckRunState) error
	GetRepoPermission(ctx context.Context, owner, repo string, username string) (string, error)
	CreateCommentReaction(ctx context.Context, owner, repo string, commentID int64, reaction string) error
}

// HasWriteAccess reports whether a GitHub collaborator permission level allows write operations.
func HasWriteAccess(permission string) bool {
	p := strings.ToLower(strings.TrimSpace(permission))
	return p == "admin" || p == "write" || p == "maintain"
}

// RateLimitError represents a rate limit error (429 or 403 with rate limit headers).
type RateLimitError struct {
	StatusCode int
	RetryAfter time.Duration
	Message    string
}

func (e *RateLimitError) Error() string {
	return e.Message
}

// ServerError represents a temporary 5xx error from GitHub.
type ServerError struct {
	StatusCode int
	Message    string
}

func (e *ServerError) Error() string {
	return e.Message
}

// PermanentError represents a non-retryable 4xx error (e.g. 404, 403, 422).
type PermanentError struct {
	StatusCode int
	Message    string
}

func (e *PermanentError) Error() string {
	return e.Message
}

// IsPermanent returns true if err is a PermanentError.
func IsPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}

// IsRetryable returns true if err is a RateLimitError or ServerError.
func IsRetryable(err error) bool {
	var rle *RateLimitError
	if errors.As(err, &rle) {
		return true
	}
	var se *ServerError
	return errors.As(err, &se)
}

// HTTPClientConfig holds configuration for HTTPClient.
type HTTPClientConfig struct {
	Token      string
	AppAuth    *AppTokenProvider
	BaseURL    string
	HTTPClient *http.Client
}

// HTTPClient implements Client using the GitHub REST API.
type HTTPClient struct {
	token      string
	appAuth    *AppTokenProvider
	baseURL    string
	httpClient *http.Client

	userMu     sync.Mutex
	cachedUser string
}

// NewHTTPClient returns a new HTTPClient.
func NewHTTPClient(cfg HTTPClientConfig) *HTTPClient {
	baseURL := strings.TrimRight(cfg.BaseURL, "/")
	if baseURL == "" {
		baseURL = "https://api.github.com"
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &HTTPClient{
		token:      cfg.Token,
		appAuth:    cfg.AppAuth,
		baseURL:    baseURL,
		httpClient: httpClient,
	}
}

func (c *HTTPClient) doRequest(ctx context.Context, method, path string, body any, out any) error {
	_, err := c.doRequestHeader(ctx, method, path, body, out)
	return err
}

func (c *HTTPClient) doRequestHeader(ctx context.Context, method, path string, body any, out any) (http.Header, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	url := path
	if !strings.HasPrefix(path, "http://") && !strings.HasPrefix(path, "https://") {
		url = fmt.Sprintf("%s%s", c.baseURL, path)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}

	token := c.token
	if c.appAuth != nil {
		tok, err := c.appAuth.Token(ctx)
		if err != nil {
			return nil, err
		}
		token = tok
	}

	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, &ServerError{StatusCode: 0, Message: fmt.Sprintf("github request failed: %v", err)}
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			_ = closeErr
		}
	}()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.Header, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.Header, c.classifyError(resp.StatusCode, resp.Header, string(respBytes))
	}

	if out != nil && len(respBytes) > 0 {
		if err := json.Unmarshal(respBytes, out); err != nil {
			return resp.Header, fmt.Errorf("failed to unmarshal response: %w", err)
		}
	}

	return resp.Header, nil
}

func (c *HTTPClient) classifyError(status int, headers http.Header, body string) error {
	cleanBody := strings.TrimSpace(body)
	if len(cleanBody) > 256 {
		cleanBody = cleanBody[:256] + "..."
	}

	var retryAfter time.Duration
	if raw := headers.Get("Retry-After"); raw != "" {
		if seconds, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil && seconds > 0 {
			retryAfter = time.Duration(seconds) * time.Second
		}
	}

	if status == http.StatusTooManyRequests {
		return &RateLimitError{StatusCode: status, RetryAfter: retryAfter, Message: fmt.Sprintf("github rate limit exceeded (429): %s", cleanBody)}
	}

	if status == http.StatusForbidden {
		if headers.Get("X-RateLimit-Remaining") == "0" || strings.Contains(strings.ToLower(body), "rate limit") {
			return &RateLimitError{StatusCode: status, RetryAfter: retryAfter, Message: fmt.Sprintf("github rate limit reached (403): %s", cleanBody)}
		}
		return &PermanentError{StatusCode: status, Message: fmt.Sprintf("github forbidden (403): %s", cleanBody)}
	}

	if status == http.StatusNotFound {
		return &PermanentError{StatusCode: status, Message: fmt.Sprintf("github not found (404): %s", cleanBody)}
	}

	if status == http.StatusUnprocessableEntity {
		return &PermanentError{StatusCode: status, Message: fmt.Sprintf("github unprocessable entity (422): %s", cleanBody)}
	}

	if status >= 500 {
		return &ServerError{StatusCode: status, Message: fmt.Sprintf("github server error (%d): %s", status, cleanBody)}
	}

	return &PermanentError{StatusCode: status, Message: fmt.Sprintf("github error (%d): %s", status, cleanBody)}
}

type userResponse struct {
	Login string `json:"login"`
}

// CurrentUser returns the authenticated login (for token auth) or bot login (for App auth).
func (c *HTTPClient) CurrentUser(ctx context.Context) (string, error) {
	if c.appAuth != nil {
		return c.appAuth.Identity(ctx)
	}

	c.userMu.Lock()
	defer c.userMu.Unlock()
	if c.cachedUser != "" {
		return c.cachedUser, nil
	}

	var resp userResponse
	if err := c.doRequest(ctx, http.MethodGet, "/user", nil, &resp); err != nil {
		return "", err
	}
	c.cachedUser = resp.Login
	return c.cachedUser, nil
}

func parseNextLink(linkHeader string) string {
	for _, part := range strings.Split(linkHeader, ",") {
		section := strings.Split(part, ";")
		if len(section) < 2 {
			continue
		}
		isNext := false
		for _, param := range section[1:] {
			param = strings.TrimSpace(param)
			if param == `rel="next"` || param == `rel=next` {
				isNext = true
				break
			}
		}
		if isNext {
			urlPart := strings.TrimSpace(section[0])
			if strings.HasPrefix(urlPart, "<") && strings.HasSuffix(urlPart, ">") {
				return strings.Trim(urlPart, "<>")
			}
		}
	}
	return ""
}

func (c *HTTPClient) ListIssueComments(ctx context.Context, owner, repo string, number int) ([]IssueComment, error) {
	path := fmt.Sprintf("/repos/%s/%s/issues/%d/comments?per_page=100", owner, repo, number)
	var allComments []IssueComment
	const maxPages = 100
	for page := 0; page < maxPages && path != ""; page++ {
		var pageComments []IssueComment
		headers, err := c.doRequestHeader(ctx, http.MethodGet, path, nil, &pageComments)
		if err != nil {
			return nil, err
		}
		allComments = append(allComments, pageComments...)
		next := parseNextLink(headers.Get("Link"))
		if next != "" {
			if strings.HasPrefix(next, "http://") || strings.HasPrefix(next, "https://") {
				if !strings.HasPrefix(next, c.baseURL) {
					return nil, fmt.Errorf("refusing to follow link header outside base URL %q: %s", c.baseURL, next)
				}
			}
		}
		path = next
	}
	return allComments, nil
}

type commentPayload struct {
	Body string `json:"body"`
}

type commentResponse struct {
	ID int64 `json:"id"`
}

func (c *HTTPClient) CreateIssueComment(ctx context.Context, owner, repo string, number int, body string) (int64, error) {
	path := fmt.Sprintf("/repos/%s/%s/issues/%d/comments", owner, repo, number)
	var resp commentResponse
	if err := c.doRequest(ctx, http.MethodPost, path, commentPayload{Body: body}, &resp); err != nil {
		return 0, err
	}
	return resp.ID, nil
}

func (c *HTTPClient) UpdateIssueComment(ctx context.Context, owner, repo string, commentID int64, body string) error {
	path := fmt.Sprintf("/repos/%s/%s/issues/comments/%d", owner, repo, commentID)
	return c.doRequest(ctx, http.MethodPatch, path, commentPayload{Body: body}, nil)
}

type checkRunOutput struct {
	Title   string `json:"title"`
	Summary string `json:"summary"`
}

type checkRunCreatePayload struct {
	Name       string          `json:"name"`
	HeadSHA    string          `json:"head_sha"`
	Status     string          `json:"status"`
	Conclusion string          `json:"conclusion,omitempty"`
	Output     *checkRunOutput `json:"output,omitempty"`
}

type checkRunUpdatePayload struct {
	Name       string          `json:"name"`
	Status     string          `json:"status"`
	Conclusion string          `json:"conclusion,omitempty"`
	Output     *checkRunOutput `json:"output,omitempty"`
}

type checkRunResponse struct {
	ID int64 `json:"id"`
}

func (c *HTTPClient) CreateCheckRun(ctx context.Context, owner, repo string, headSHA string, state CheckRunState) (int64, error) {
	path := fmt.Sprintf("/repos/%s/%s/check-runs", owner, repo)
	payload := checkRunCreatePayload{
		Name:       state.Name,
		HeadSHA:    headSHA,
		Status:     state.Status,
		Conclusion: state.Conclusion,
		Output: &checkRunOutput{
			Title:   state.Title,
			Summary: state.Summary,
		},
	}
	var resp checkRunResponse
	if err := c.doRequest(ctx, http.MethodPost, path, payload, &resp); err != nil {
		return 0, err
	}
	return resp.ID, nil
}

func (c *HTTPClient) UpdateCheckRun(ctx context.Context, owner, repo string, checkRunID int64, state CheckRunState) error {
	path := fmt.Sprintf("/repos/%s/%s/check-runs/%d", owner, repo, checkRunID)
	payload := checkRunUpdatePayload{
		Name:       state.Name,
		Status:     state.Status,
		Conclusion: state.Conclusion,
		Output: &checkRunOutput{
			Title:   state.Title,
			Summary: state.Summary,
		},
	}
	return c.doRequest(ctx, http.MethodPatch, path, payload, nil)
}

type repoPermissionResponse struct {
	Permission string `json:"permission"`
	RoleName   string `json:"role_name"`
}

func (c *HTTPClient) GetRepoPermission(ctx context.Context, owner, repo string, username string) (string, error) {
	path := fmt.Sprintf("/repos/%s/%s/collaborators/%s/permission", owner, repo, username)
	var resp repoPermissionResponse
	if err := c.doRequest(ctx, http.MethodGet, path, nil, &resp); err != nil {
		var permErr *PermanentError
		if errors.As(err, &permErr) && permErr.StatusCode == http.StatusNotFound {
			return "none", nil
		}
		return "", err
	}
	if resp.Permission != "" {
		return resp.Permission, nil
	}
	return resp.RoleName, nil
}

type reactionPayload struct {
	Content string `json:"content"`
}

func (c *HTTPClient) CreateCommentReaction(ctx context.Context, owner, repo string, commentID int64, reaction string) error {
	path := fmt.Sprintf("/repos/%s/%s/issues/comments/%d/reactions", owner, repo, commentID)
	return c.doRequest(ctx, http.MethodPost, path, reactionPayload{Content: reaction}, nil)
}
