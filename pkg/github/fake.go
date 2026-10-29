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
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FakeCheckRun records check run details in the fake server.
type FakeCheckRun struct {
	ID         int64
	Name       string
	HeadSHA    string
	Status     string
	Conclusion string
	Title      string
	Summary    string
}

// FakeGitHubServer is an in-memory httptest fake simulating the GitHub REST API.
type FakeGitHubServer struct {
	mu             sync.Mutex
	server         *httptest.Server
	comments       map[int64]*IssueComment
	issueComments  map[string][]int64 // "owner/repo#number" -> comment IDs
	checkRuns      map[int64]*FakeCheckRun
	permissions    map[string]string   // "owner/repo/user" -> permission level
	reactions      map[int64][]string  // commentID -> reactions
	prFiles        map[string][]string // "owner/repo#number" -> list of filenames
	nextCommentID  int64
	nextCheckRunID int64
	currentUser    string
	appSlug        string

	// Injected errors: if non-empty, the next request(s) return this status and message
	injectedErrors []injectedError
	requestLog     []string
}

type injectedError struct {
	method     string
	pathPrefix string
	statusCode int
	body       string
	headers    map[string]string
}

// NewFakeGitHubServer creates and starts a new FakeGitHubServer.
func NewFakeGitHubServer() *FakeGitHubServer {
	f := &FakeGitHubServer{
		comments:       make(map[int64]*IssueComment),
		issueComments:  make(map[string][]int64),
		checkRuns:      make(map[int64]*FakeCheckRun),
		permissions:    make(map[string]string),
		reactions:      make(map[int64][]string),
		prFiles:        make(map[string][]string),
		nextCommentID:  1000,
		nextCheckRunID: 5000,
		currentUser:    "test-user",
		appSlug:        "eval-runner",
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.handleHTTP))
	return f
}

// URL returns the base URL of the fake server.
func (f *FakeGitHubServer) URL() string {
	return f.server.URL
}

// Close shuts down the fake server.
func (f *FakeGitHubServer) Close() {
	f.server.Close()
}

// SetCurrentUser sets the login returned by GET /user.
func (f *FakeGitHubServer) SetCurrentUser(login string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.currentUser = login
}

// SetAppSlug sets the slug returned by GET /app.
func (f *FakeGitHubServer) SetAppSlug(slug string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.appSlug = slug
}

// SetUserPermission sets collaborator permission for a user in owner/repo.
func (f *FakeGitHubServer) SetUserPermission(owner, repo, user, permission string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := fmt.Sprintf("%s/%s/%s", strings.ToLower(owner), strings.ToLower(repo), strings.ToLower(user))
	f.permissions[key] = permission
}

// CommentReactions returns all reactions created on commentID.
func (f *FakeGitHubServer) CommentReactions(commentID int64) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.reactions[commentID]...)
}

// Client returns an HTTPClient configured to use this fake server.
func (f *FakeGitHubServer) Client(token string) *HTTPClient {
	return NewHTTPClient(HTTPClientConfig{
		Token:      token,
		BaseURL:    f.server.URL,
		HTTPClient: f.server.Client(),
	})
}

// AppClient returns an HTTPClient configured with AppAuth pointing to this fake server.
func (f *FakeGitHubServer) AppClient(appID, instID string, privKey []byte) (*HTTPClient, error) {
	appAuth, err := NewAppTokenProvider(AppConfig{
		AppID:          appID,
		InstallationID: instID,
		PrivateKeyPEM:  privKey,
		BaseURL:        f.server.URL,
		HTTPClient:     f.server.Client(),
	})
	if err != nil {
		return nil, err
	}
	return NewHTTPClient(HTTPClientConfig{
		AppAuth:    appAuth,
		BaseURL:    f.server.URL,
		HTTPClient: f.server.Client(),
	}), nil
}

// InjectError configures the next HTTP request to return the given status code and body.
func (f *FakeGitHubServer) InjectError(statusCode int, body string, headers ...map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var h map[string]string
	if len(headers) > 0 {
		h = headers[0]
	}
	f.injectedErrors = append(f.injectedErrors, injectedError{
		statusCode: statusCode,
		body:       body,
		headers:    h,
	})
}

// InjectPathError configures a status code and body to be returned for a request matching method and pathPrefix.
func (f *FakeGitHubServer) InjectPathError(method, pathPrefix string, statusCode int, body string, headers ...map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var h map[string]string
	if len(headers) > 0 {
		h = headers[0]
	}
	f.injectedErrors = append(f.injectedErrors, injectedError{
		method:     method,
		pathPrefix: pathPrefix,
		statusCode: statusCode,
		body:       body,
		headers:    h,
	})
}

// SetPullRequestFiles sets the list of files returned for GET /repos/{owner}/{repo}/pulls/{number}/files.
func (f *FakeGitHubServer) SetPullRequestFiles(owner, repo string, number int, files []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := fmt.Sprintf("%s/%s#%d", strings.ToLower(owner), strings.ToLower(repo), number)
	f.prFiles[key] = append([]string(nil), files...)
}

// AddCommentWithAuthor creates a comment directly in the fake server under the given author.
func (f *FakeGitHubServer) AddCommentWithAuthor(owner, repo string, number int, body string, author string) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextCommentID++
	c := &IssueComment{
		ID:   f.nextCommentID,
		Body: body,
		User: IssueCommentUser{Login: author},
	}
	f.comments[c.ID] = c
	issueKey := fmt.Sprintf("%s/%s#%d", owner, repo, number)
	f.issueComments[issueKey] = append(f.issueComments[issueKey], c.ID)
	return c.ID
}

// Comments returns all stored comments.
func (f *FakeGitHubServer) Comments() []*IssueComment {
	f.mu.Lock()
	defer f.mu.Unlock()
	var res []*IssueComment
	for _, c := range f.comments {
		res = append(res, c)
	}
	return res
}

// CheckRuns returns all stored check runs.
func (f *FakeGitHubServer) CheckRuns() []*FakeCheckRun {
	f.mu.Lock()
	defer f.mu.Unlock()
	var res []*FakeCheckRun
	for _, cr := range f.checkRuns {
		res = append(res, cr)
	}
	return res
}

// RequestLog returns logged request method and paths.
func (f *FakeGitHubServer) RequestLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	copied := make([]string, len(f.requestLog))
	copy(copied, f.requestLog)
	return copied
}

func writeResponse(w http.ResponseWriter, statusCode int, contentType string, body []byte) {
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	if statusCode != 0 {
		w.WriteHeader(statusCode)
	}
	if _, err := w.Write(body); err != nil {
		log.Printf("fake github server: failed to write response: %v", err)
	}
}

func writeJSONResponse(w http.ResponseWriter, statusCode int, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeResponse(w, statusCode, "application/json", append(data, '\n'))
}

func (f *FakeGitHubServer) handleHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requestLog = append(f.requestLog, fmt.Sprintf("%s %s", r.Method, r.URL.Path))

	for i, errResp := range f.injectedErrors {
		if (errResp.method == "" || r.Method == errResp.method) &&
			(errResp.pathPrefix == "" || strings.HasPrefix(r.URL.Path, errResp.pathPrefix)) {
			f.injectedErrors = append(f.injectedErrors[:i], f.injectedErrors[i+1:]...)
			for k, v := range errResp.headers {
				w.Header().Set(k, v)
			}
			writeResponse(w, errResp.statusCode, "", []byte(errResp.body))
			f.mu.Unlock()
			return
		}
	}
	f.mu.Unlock()

	path := r.URL.Path

	// 0a. Current user: /user
	if path == "/user" && r.Method == http.MethodGet {
		f.mu.Lock()
		login := f.currentUser
		f.mu.Unlock()
		writeJSONResponse(w, http.StatusOK, map[string]any{"login": login})
		return
	}

	// 0b. App info: /app
	if path == "/app" && r.Method == http.MethodGet {
		f.mu.Lock()
		slug := f.appSlug
		f.mu.Unlock()
		writeJSONResponse(w, http.StatusOK, map[string]any{"id": 1, "slug": slug})
		return
	}

	// 0c. App installation access tokens: /app/installations/{id}/access_tokens
	if strings.HasPrefix(path, "/app/installations/") && strings.HasSuffix(path, "/access_tokens") && r.Method == http.MethodPost {
		writeJSONResponse(w, http.StatusCreated, installationTokenResponse{
			Token:     "ghs_fake_installation_token",
			ExpiresAt: time.Now().Add(1 * time.Hour),
		})
		return
	}

	// 1. List or create issue comments: /repos/{owner}/{repo}/issues/{number}/comments
	if strings.HasPrefix(path, "/repos/") && strings.Contains(path, "/issues/") && strings.HasSuffix(path, "/comments") {
		f.handleIssueComments(w, r)
		return
	}

	// 1b. Comment reaction: /repos/{owner}/{repo}/issues/comments/{comment_id}/reactions
	if strings.HasPrefix(path, "/repos/") && strings.Contains(path, "/issues/comments/") && strings.HasSuffix(path, "/reactions") && r.Method == http.MethodPost {
		f.handleCreateReaction(w, r)
		return
	}

	// 2. Update issue comment: /repos/{owner}/{repo}/issues/comments/{comment_id}
	if strings.HasPrefix(path, "/repos/") && strings.Contains(path, "/issues/comments/") {
		f.handleUpdateComment(w, r)
		return
	}

	// 2b. Collaborator permission: /repos/{owner}/{repo}/collaborators/{username}/permission
	if strings.HasPrefix(path, "/repos/") && strings.Contains(path, "/collaborators/") && strings.HasSuffix(path, "/permission") && r.Method == http.MethodGet {
		f.handleGetPermission(w, r)
		return
	}

	// 3. Create check run: /repos/{owner}/{repo}/check-runs
	if strings.HasPrefix(path, "/repos/") && strings.HasSuffix(path, "/check-runs") && r.Method == http.MethodPost {
		f.handleCreateCheckRun(w, r)
		return
	}

	// 4. Update check run: /repos/{owner}/{repo}/check-runs/{check_run_id}
	if strings.HasPrefix(path, "/repos/") && strings.Contains(path, "/check-runs/") && r.Method == http.MethodPatch {
		f.handleUpdateCheckRun(w, r)
		return
	}

	// 5. List pull request files: /repos/{owner}/{repo}/pulls/{number}/files
	if strings.HasPrefix(path, "/repos/") && strings.Contains(path, "/pulls/") && strings.HasSuffix(path, "/files") && r.Method == http.MethodGet {
		f.handleListPullRequestFiles(w, r)
		return
	}

	http.NotFound(w, r)
}

func (f *FakeGitHubServer) handleListPullRequestFiles(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// ["repos", owner, repo, "pulls", number, "files"]
	if len(parts) != 6 || parts[0] != "repos" || parts[3] != "pulls" || parts[5] != "files" {
		http.NotFound(w, r)
		return
	}
	owner := parts[1]
	repo := parts[2]
	number, err := strconv.Atoi(parts[4])
	if err != nil {
		http.NotFound(w, r)
		return
	}

	key := fmt.Sprintf("%s/%s#%d", strings.ToLower(owner), strings.ToLower(repo), number)
	files := f.prFiles[key]

	perPage := 100
	if pp := r.URL.Query().Get("per_page"); pp != "" {
		if val, err := strconv.Atoi(pp); err == nil && val > 0 {
			perPage = val
		}
	}
	page := 1
	if p := r.URL.Query().Get("page"); p != "" {
		if val, err := strconv.Atoi(p); err == nil && val > 0 {
			page = val
		}
	}

	start := (page - 1) * perPage
	if start > len(files) {
		start = len(files)
	}
	end := start + perPage
	if end > len(files) {
		end = len(files)
	}

	if end < len(files) {
		nextURL := fmt.Sprintf("%s%s?page=%d&per_page=%d", f.server.URL, r.URL.Path, page+1, perPage)
		w.Header().Set("Link", fmt.Sprintf("<%s>; rel=\"next\"", nextURL))
	}

	var resp []pullRequestFileResponse
	for _, filename := range files[start:end] {
		resp = append(resp, pullRequestFileResponse{Filename: filename})
	}
	if resp == nil {
		resp = []pullRequestFileResponse{}
	}

	writeJSONResponse(w, http.StatusOK, resp)
}

func (f *FakeGitHubServer) handleIssueComments(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// ["repos", owner, repo, "issues", number, "comments"]
	if len(parts) != 6 {
		http.NotFound(w, r)
		return
	}
	owner, repo, numStr := parts[1], parts[2], parts[4]
	issueKey := fmt.Sprintf("%s/%s#%s", owner, repo, numStr)

	switch r.Method {
	case http.MethodGet:
		ids := f.issueComments[issueKey]
		var result []*IssueComment
		for _, id := range ids {
			if c, ok := f.comments[id]; ok {
				result = append(result, c)
			}
		}

		q := r.URL.Query()
		perPage := 100
		if p := q.Get("per_page"); p != "" {
			if val, err := strconv.Atoi(p); err == nil && val > 0 {
				perPage = val
			}
		}
		page := 1
		if p := q.Get("page"); p != "" {
			if val, err := strconv.Atoi(p); err == nil && val > 0 {
				page = val
			}
		}

		start := (page - 1) * perPage
		if start > len(result) {
			start = len(result)
		}
		end := start + perPage
		if end > len(result) {
			end = len(result)
		}

		if end < len(result) {
			nextURL := fmt.Sprintf("%s%s?page=%d&per_page=%d", f.server.URL, r.URL.Path, page+1, perPage)
			w.Header().Set("Link", fmt.Sprintf("<%s>; rel=\"next\"", nextURL))
		}

		writeJSONResponse(w, http.StatusOK, result[start:end])

	case http.MethodPost:
		var payload commentPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.nextCommentID++
		login := f.currentUser
		if login == "" {
			login = "test-user"
		}
		c := &IssueComment{
			ID:   f.nextCommentID,
			Body: payload.Body,
			User: IssueCommentUser{Login: login},
		}
		f.comments[c.ID] = c
		f.issueComments[issueKey] = append(f.issueComments[issueKey], c.ID)

		writeJSONResponse(w, http.StatusCreated, commentResponse{ID: c.ID})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (f *FakeGitHubServer) handleUpdateComment(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if r.Method != http.MethodPatch {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// ["repos", owner, repo, "issues", "comments", comment_id]
	if len(parts) != 6 {
		http.NotFound(w, r)
		return
	}
	id, err := strconv.ParseInt(parts[5], 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	c, ok := f.comments[id]
	if !ok {
		http.NotFound(w, r)
		return
	}

	var payload commentPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	c.Body = payload.Body

	writeJSONResponse(w, http.StatusOK, c)
}

func (f *FakeGitHubServer) handleCreateCheckRun(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var payload checkRunCreatePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	f.nextCheckRunID++
	cr := &FakeCheckRun{
		ID:         f.nextCheckRunID,
		Name:       payload.Name,
		HeadSHA:    payload.HeadSHA,
		Status:     payload.Status,
		Conclusion: payload.Conclusion,
	}
	if payload.Output != nil {
		cr.Title = payload.Output.Title
		cr.Summary = payload.Output.Summary
	}
	f.checkRuns[cr.ID] = cr

	writeJSONResponse(w, http.StatusCreated, checkRunResponse{ID: cr.ID})
}

func (f *FakeGitHubServer) handleUpdateCheckRun(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// ["repos", owner, repo, "check-runs", check_run_id]
	if len(parts) != 5 {
		http.NotFound(w, r)
		return
	}
	id, err := strconv.ParseInt(parts[4], 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	cr, ok := f.checkRuns[id]
	if !ok {
		http.NotFound(w, r)
		return
	}

	var payload checkRunUpdatePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	cr.Name = payload.Name
	cr.Status = payload.Status
	cr.Conclusion = payload.Conclusion
	if payload.Output != nil {
		cr.Title = payload.Output.Title
		cr.Summary = payload.Output.Summary
	}

	writeJSONResponse(w, http.StatusOK, checkRunResponse{ID: cr.ID})
}

func (f *FakeGitHubServer) handleGetPermission(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// ["repos", owner, repo, "collaborators", username, "permission"]
	if len(parts) != 6 {
		http.NotFound(w, r)
		return
	}
	owner, repo, user := strings.ToLower(parts[1]), strings.ToLower(parts[2]), strings.ToLower(parts[4])
	key := fmt.Sprintf("%s/%s/%s", owner, repo, user)
	perm, ok := f.permissions[key]
	if !ok {
		writeJSONResponse(w, http.StatusNotFound, map[string]string{"message": "User is not a collaborator"})
		return
	}

	writeJSONResponse(w, http.StatusOK, map[string]string{
		"permission": perm,
		"role_name":  perm,
	})
}

func (f *FakeGitHubServer) handleCreateReaction(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// ["repos", owner, repo, "issues", "comments", commentID, "reactions"]
	if len(parts) != 7 {
		http.NotFound(w, r)
		return
	}
	id, err := strconv.ParseInt(parts[5], 10, 64)
	if err != nil {
		http.Error(w, "invalid comment id", http.StatusBadRequest)
		return
	}

	var payload reactionPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	f.reactions[id] = append(f.reactions[id], payload.Content)
	writeJSONResponse(w, http.StatusCreated, map[string]any{
		"content": payload.Content,
	})
}
