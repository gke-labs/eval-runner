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
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/eval-runner/pkg/forge"
	"github.com/gke-labs/eval-runner/pkg/forge/store"
	gh "github.com/gke-labs/eval-runner/pkg/github"
)

var (
	_ forge.Forge    = (*Forge)(nil)
	_ forge.Reporter = (*Forge)(nil)

	// ErrCheckRunsUnavailable indicates that check runs cannot be created with token authentication.
	ErrCheckRunsUnavailable = errors.New("check runs unavailable with token auth; comment posted")
)

// Store defines the store methods required by the GitHub forge for conditional ETag caching.
type Store interface {
	GetEndpointSync(repo forge.RepoRef, endpoint string) (*store.EndpointSync, error)
	GetRepository(repo forge.RepoRef) (*forge.Repository, bool)
	ListBranches(repo forge.RepoRef) ([]forge.Branch, error)
	GetOpenChangeRequests(repo forge.RepoRef) []forge.ChangeRequest
	GetChangeRequest(repo forge.RepoRef, numberOrID string) (*forge.ChangeRequest, error)
	GetCommit(sha string) (*forge.Commit, bool)
}

// Forge implements forge.Forge and forge.Reporter for github.com.
type Forge struct {
	mu           sync.RWMutex
	client       *gh.HTTPClient
	store        Store
	pendingSyncs map[string]map[string]*store.EndpointSync
}

// Client returns the underlying gh.HTTPClient.
func (f *Forge) Client() *gh.HTTPClient {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.client
}

// RateLimit returns the current rate limit info from the underlying client.
func (f *Forge) RateLimit() gh.RateLimitInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.client != nil {
		return f.client.RateLimit()
	}
	return gh.RateLimitInfo{}
}

// SetStore sets or updates the backing store for ETag conditional caching.
func (f *Forge) SetStore(s Store) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.store = s
}

func (f *Forge) stageEndpointSync(ref forge.RepoRef, endpoint string, etag, lastMod string) {
	if etag == "" && lastMod == "" {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pendingSyncs == nil {
		f.pendingSyncs = make(map[string]map[string]*store.EndpointSync)
	}
	rKey := strings.ToLower(ref.String())
	if f.pendingSyncs[rKey] == nil {
		f.pendingSyncs[rKey] = make(map[string]*store.EndpointSync)
	}
	f.pendingSyncs[rKey][endpoint] = &store.EndpointSync{
		ETag:         etag,
		LastModified: lastMod,
	}
}

// PopPendingEndpointSyncs returns any new endpoint sync validators collected for ref
// and clears them from pending.
func (f *Forge) PopPendingEndpointSyncs(ref forge.RepoRef) map[string]*store.EndpointSync {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pendingSyncs == nil {
		return nil
	}
	rKey := strings.ToLower(ref.String())
	syncs := f.pendingSyncs[rKey]
	delete(f.pendingSyncs, rKey)
	return syncs
}

// ClearPendingEndpointSyncs discards pending endpoint sync validators for ref (e.g. on tick failure).
func (f *Forge) ClearPendingEndpointSyncs(ref forge.RepoRef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pendingSyncs != nil {
		delete(f.pendingSyncs, strings.ToLower(ref.String()))
	}
}

// Option configures a GitHub Forge.
type Option func(*options)

type options struct {
	client     *gh.HTTPClient
	token      string
	appAuth    *gh.AppTokenProvider
	baseURL    string
	httpClient *http.Client
	store      Store
}

// WithClient configures an existing gh.HTTPClient.
func WithClient(client *gh.HTTPClient) Option {
	return func(o *options) {
		o.client = client
	}
}

// WithStore configures a backing store for ETag conditional requests.
func WithStore(s Store) Option {
	return func(o *options) {
		o.store = s
	}
}

// WithToken sets an API token for GitHub authentication.
func WithToken(token string) Option {
	return func(o *options) {
		o.token = token
	}
}

// WithAppAuth sets GitHub App token authentication.
func WithAppAuth(appAuth *gh.AppTokenProvider) Option {
	return func(o *options) {
		o.appAuth = appAuth
	}
}

// WithBaseURL overrides the GitHub API base URL (useful for enterprise or tests).
func WithBaseURL(baseURL string) Option {
	return func(o *options) {
		o.baseURL = baseURL
	}
}

// WithHTTPClient sets a custom underlying http.Client.
func WithHTTPClient(client *http.Client) Option {
	return func(o *options) {
		o.httpClient = client
	}
}

// New creates a new GitHub Forge instance.
func New(opts ...Option) *Forge {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	if o.client != nil {
		return &Forge{client: o.client, store: o.store}
	}

	client := gh.NewHTTPClient(gh.HTTPClientConfig{
		Token:      o.token,
		AppAuth:    o.appAuth,
		BaseURL:    o.baseURL,
		HTTPClient: o.httpClient,
	})
	return &Forge{client: client, store: o.store}
}

// Kind returns "github.com".
func (f *Forge) Kind() string {
	return "github.com"
}

type repoResponse struct {
	DefaultBranch string `json:"default_branch"`
	CloneURL      string `json:"clone_url"`
}

// Repository returns metadata for a repository reference.
func (f *Forge) Repository(ctx context.Context, ref forge.RepoRef) (*forge.Repository, error) {
	path := fmt.Sprintf("/repos/%s/%s", ref.Owner, ref.Name)
	var etag, lastMod string
	f.mu.RLock()
	st := f.store
	f.mu.RUnlock()
	if st != nil {
		if ep, err := st.GetEndpointSync(ref, path); err == nil && ep != nil {
			etag = ep.ETag
			lastMod = ep.LastModified
		}
	}

	var resp repoResponse
	headers, notModified, err := f.client.DoRequestConditional(ctx, http.MethodGet, path, nil, &resp, etag, lastMod)
	if err != nil {
		return nil, mapError(err)
	}
	if notModified {
		if st != nil {
			if r, ok := st.GetRepository(ref); ok && r != nil {
				return r, nil
			}
		}
		return nil, nil
	}

	f.stageEndpointSync(ref, path, headers.Get("ETag"), headers.Get("Last-Modified"))

	return &forge.Repository{
		Ref:           ref,
		DefaultBranch: resp.DefaultBranch,
		CloneURL:      resp.CloneURL,
	}, nil
}

type branchResponse struct {
	Name   string `json:"name"`
	Commit struct {
		SHA string `json:"sha"`
	} `json:"commit"`
}

// Branches lists branch heads (name -> commit sha) with pagination.
func (f *Forge) Branches(ctx context.Context, ref forge.RepoRef) ([]forge.Branch, error) {
	path := fmt.Sprintf("/repos/%s/%s/branches?per_page=100", ref.Owner, ref.Name)
	var allBranches []forge.Branch
	const maxPages = 100

	f.mu.RLock()
	st := f.store
	f.mu.RUnlock()

	for page := 0; page < maxPages && path != ""; page++ {
		var etag, lastMod string
		if page == 0 && st != nil {
			if ep, err := st.GetEndpointSync(ref, path); err == nil && ep != nil {
				etag = ep.ETag
				lastMod = ep.LastModified
			}
		}

		var pageBranches []branchResponse
		headers, notModified, err := f.client.DoRequestConditional(ctx, http.MethodGet, path, nil, &pageBranches, etag, lastMod)
		if err != nil {
			return nil, mapError(err)
		}
		if notModified {
			if st != nil {
				return st.ListBranches(ref)
			}
			return nil, nil
		}

		if page == 0 {
			f.stageEndpointSync(ref, path, headers.Get("ETag"), headers.Get("Last-Modified"))
		}

		for _, b := range pageBranches {
			allBranches = append(allBranches, forge.Branch{
				Name: b.Name,
				SHA:  b.Commit.SHA,
			})
		}

		next := gh.ParseNextLink(headers.Get("Link"))
		if next != "" {
			if strings.HasPrefix(next, "http://") || strings.HasPrefix(next, "https://") {
				baseURL := f.client.BaseURL()
				if !strings.HasPrefix(next, baseURL) {
					return nil, fmt.Errorf("refusing to follow link header outside base URL %q: %s", baseURL, next)
				}
			}
		}
		path = next
	}

	return allBranches, nil
}

type pullResponse struct {
	ID     int64  `json:"id"`
	Number int    `json:"number"`
	Title  string `json:"title"`
	User   struct {
		Login string `json:"login"`
	} `json:"user"`
	Head struct {
		Ref  string `json:"ref"`
		SHA  string `json:"sha"`
		Repo *struct {
			CloneURL string `json:"clone_url"`
			FullName string `json:"full_name"`
			Name     string `json:"name"`
			Owner    struct {
				Login string `json:"login"`
			} `json:"owner"`
		} `json:"repo"`
	} `json:"head"`
	Base struct {
		Ref string `json:"ref"`
		SHA string `json:"sha"`
	} `json:"base"`
	State     string     `json:"state"`
	Merged    bool       `json:"merged,omitempty"`
	MergedAt  *time.Time `json:"merged_at,omitempty"`
	UpdatedAt time.Time  `json:"updated_at"`
}

func convertPullResponse(ref forge.RepoRef, pr pullResponse) forge.ChangeRequest {
	headRepoRef := ref
	var headCloneURL string
	if pr.Head.Repo != nil {
		headCloneURL = pr.Head.Repo.CloneURL
		if parsed, ok := forge.ParseRepoURL(pr.Head.Repo.CloneURL); ok {
			headRepoRef = parsed
		} else if pr.Head.Repo.FullName != "" {
			if parsed, ok := forge.RepoRefFromOwnerName(pr.Head.Repo.FullName); ok {
				headRepoRef = parsed
			}
		} else if pr.Head.Repo.Owner.Login != "" && pr.Head.Repo.Name != "" {
			headRepoRef = forge.RepoRef{
				Host:  ref.Host,
				Owner: pr.Head.Repo.Owner.Login,
				Name:  pr.Head.Repo.Name,
			}
		}
	}

	isMerged := pr.Merged || pr.MergedAt != nil

	return forge.ChangeRequest{
		ID:     forge.ChangeRequestID(strconv.Itoa(pr.Number)),
		Number: pr.Number,
		Title:  pr.Title,
		Author: pr.User.Login,
		Head: forge.ChangeRequestHead{
			Ref:      pr.Head.Ref,
			SHA:      pr.Head.SHA,
			RepoRef:  headRepoRef,
			CloneURL: headCloneURL,
		},
		Base: forge.ChangeRequestBase{
			Ref: pr.Base.Ref,
			SHA: pr.Base.SHA,
		},
		State:     pr.State,
		Merged:    isMerged,
		UpdatedAt: pr.UpdatedAt,
	}
}

// PullRequest retrieves a single change request by number.
func (f *Forge) PullRequest(ctx context.Context, ref forge.RepoRef, number int) (*forge.ChangeRequest, error) {
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d", ref.Owner, ref.Name, number)
	var etag, lastMod string
	f.mu.RLock()
	st := f.store
	f.mu.RUnlock()
	if st != nil {
		if ep, err := st.GetEndpointSync(ref, path); err == nil && ep != nil {
			etag = ep.ETag
			lastMod = ep.LastModified
		}
	}

	var pr pullResponse
	headers, notModified, err := f.client.DoRequestConditional(ctx, http.MethodGet, path, nil, &pr, etag, lastMod)
	if err != nil {
		return nil, mapError(err)
	}
	if notModified {
		if st != nil {
			if cr, err := st.GetChangeRequest(ref, strconv.Itoa(number)); err == nil && cr != nil {
				return cr, nil
			}
		}
		return nil, nil
	}

	f.stageEndpointSync(ref, path, headers.Get("ETag"), headers.Get("Last-Modified"))

	cr := convertPullResponse(ref, pr)
	return &cr, nil
}

// ChangeRequests lists open change requests with head/base refs and shas.
func (f *Forge) ChangeRequests(ctx context.Context, ref forge.RepoRef) ([]forge.ChangeRequest, error) {
	path := fmt.Sprintf("/repos/%s/%s/pulls?state=open&per_page=100", ref.Owner, ref.Name)
	var allCRs []forge.ChangeRequest
	const maxPages = 100

	f.mu.RLock()
	st := f.store
	f.mu.RUnlock()

	for page := 0; page < maxPages && path != ""; page++ {
		var etag, lastMod string
		if page == 0 && st != nil {
			if ep, err := st.GetEndpointSync(ref, path); err == nil && ep != nil {
				etag = ep.ETag
				lastMod = ep.LastModified
			}
		}

		var pagePRs []pullResponse
		headers, notModified, err := f.client.DoRequestConditional(ctx, http.MethodGet, path, nil, &pagePRs, etag, lastMod)
		if err != nil {
			return nil, mapError(err)
		}
		if notModified {
			if st != nil {
				return st.GetOpenChangeRequests(ref), nil
			}
			return nil, nil
		}

		if page == 0 {
			f.stageEndpointSync(ref, path, headers.Get("ETag"), headers.Get("Last-Modified"))
		}

		for _, pr := range pagePRs {
			allCRs = append(allCRs, convertPullResponse(ref, pr))
		}

		next := gh.ParseNextLink(headers.Get("Link"))
		if next != "" {
			if strings.HasPrefix(next, "http://") || strings.HasPrefix(next, "https://") {
				baseURL := f.client.BaseURL()
				if !strings.HasPrefix(next, baseURL) {
					return nil, fmt.Errorf("refusing to follow link header outside base URL %q: %s", baseURL, next)
				}
			}
		}
		path = next
	}

	return allCRs, nil
}

type commitResponse struct {
	SHA     string `json:"sha"`
	Parents []struct {
		SHA string `json:"sha"`
	} `json:"parents"`
	Commit struct {
		Author struct {
			Date time.Time `json:"date"`
		} `json:"author"`
		Committer struct {
			Date time.Time `json:"date"`
		} `json:"committer"`
		Message string `json:"message"`
	} `json:"commit"`
}

// Commit returns commit metadata (sha, parents, author time, message title).
func (f *Forge) Commit(ctx context.Context, ref forge.RepoRef, sha string) (*forge.Commit, error) {
	f.mu.RLock()
	st := f.store
	f.mu.RUnlock()

	if st != nil {
		if sc, ok := st.GetCommit(sha); ok && sc != nil {
			return sc, nil
		}
	}

	path := fmt.Sprintf("/repos/%s/%s/commits/%s", ref.Owner, ref.Name, sha)
	var etag, lastMod string
	if st != nil {
		if ep, err := st.GetEndpointSync(ref, path); err == nil && ep != nil {
			etag = ep.ETag
			lastMod = ep.LastModified
		}
	}

	var resp commitResponse
	_, notModified, err := f.client.DoRequestConditional(ctx, http.MethodGet, path, nil, &resp, etag, lastMod)
	if err != nil {
		return nil, mapError(err)
	}
	if notModified {
		if st != nil {
			if sc, ok := st.GetCommit(sha); ok && sc != nil {
				return sc, nil
			}
		}
		return nil, nil
	}

	parents := make([]string, len(resp.Parents))
	for i, p := range resp.Parents {
		parents[i] = p.SHA
	}

	t := resp.Commit.Author.Date
	if t.IsZero() {
		t = resp.Commit.Committer.Date
	}

	title := resp.Commit.Message
	if idx := strings.Index(title, "\n"); idx != -1 {
		title = title[:idx]
	}

	return &forge.Commit{
		SHA:     resp.SHA,
		Parents: parents,
		Time:    t,
		Title:   strings.TrimSpace(title),
	}, nil
}

// UpsertComment posts or updates a PR comment matching key.
func (f *Forge) UpsertComment(ctx context.Context, ref forge.RepoRef, id forge.ChangeRequestID, key string, body string) (int64, error) {
	if !f.client.HasCredentials() {
		return 0, forge.ErrUnauthorized
	}

	prNum, err := strconv.Atoi(strings.TrimPrefix(string(id), "#"))
	if err != nil {
		return 0, fmt.Errorf("invalid change request id %q: %w", id, err)
	}

	// Plain comment without marker when key is empty
	if key == "" {
		commentID, err := f.client.CreateIssueComment(ctx, ref.Owner, ref.Name, prNum, body)
		return commentID, mapError(err)
	}

	marker := key
	if !strings.HasPrefix(marker, "<!--") {
		marker = fmt.Sprintf("<!-- %s -->", marker)
	}

	content := body
	if !strings.Contains(content, marker) {
		content = marker + "\n\n" + content
	}

	currentUser, err := f.client.CurrentUser(ctx)
	if err != nil {
		return 0, mapError(err)
	}

	comments, err := f.client.ListIssueComments(ctx, ref.Owner, ref.Name, prNum)
	if err != nil {
		return 0, mapError(err)
	}

	commentID := int64(0)
	for _, c := range comments {
		if strings.Contains(c.Body, marker) {
			if currentUser == "" || strings.EqualFold(c.User.Login, currentUser) {
				commentID = c.ID
				break
			}
		}
	}

	if commentID != 0 {
		if err := f.client.UpdateIssueComment(ctx, ref.Owner, ref.Name, commentID, content); err != nil {
			return 0, mapError(err)
		}
		return commentID, nil
	}

	newID, err := f.client.CreateIssueComment(ctx, ref.Owner, ref.Name, prNum, content)
	return newID, mapError(err)
}

// SetStatus reports a status check (check run) on a commit SHA.
func (f *Forge) SetStatus(ctx context.Context, ref forge.RepoRef, sha string, status forge.Status) (int64, error) {
	if !f.client.HasCredentials() {
		return 0, forge.ErrUnauthorized
	}

	checkState := gh.CheckRunState{
		Name:        status.Context,
		Title:       status.Description,
		Summary:     status.Description,
		Text:        status.Text,
		DetailsURL:  status.TargetURL,
		Images:      status.Images,
		Annotations: status.Annotations,
		Actions:     status.Actions,
	}
	if checkState.Name == "" {
		checkState.Name = gh.CheckRunName
	}
	if status.Status == "in_progress" || status.State == "in_progress" || status.State == "pending" {
		checkState.Status = "in_progress"
		checkState.Conclusion = ""
	} else {
		checkState.Status = "completed"
		if status.Conclusion != "" {
			checkState.Conclusion = status.Conclusion
		} else {
			checkState.Conclusion = status.State
		}
	}

	if status.CheckRunID != 0 {
		err := f.client.UpdateCheckRun(ctx, ref.Owner, ref.Name, status.CheckRunID, checkState)
		if err == nil {
			return status.CheckRunID, nil
		}
		var pe *gh.PermanentError
		if errors.As(err, &pe) {
			if pe.StatusCode == http.StatusForbidden {
				return 0, ErrCheckRunsUnavailable
			}
			if pe.StatusCode != http.StatusNotFound {
				return 0, mapError(err)
			}
		} else {
			return 0, mapError(err)
		}
	}

	newID, err := f.client.CreateCheckRun(ctx, ref.Owner, ref.Name, sha, checkState)
	if err != nil {
		var pe *gh.PermanentError
		if errors.As(err, &pe) && pe.StatusCode == http.StatusForbidden {
			return 0, ErrCheckRunsUnavailable
		}
		return 0, mapError(err)
	}
	return newID, nil
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	var pe *gh.PermanentError
	if errors.As(err, &pe) {
		if pe.StatusCode == http.StatusNotFound {
			return fmt.Errorf("%w: %w", forge.ErrNotFound, pe)
		}
		if pe.StatusCode == http.StatusUnauthorized || pe.StatusCode == http.StatusForbidden {
			return fmt.Errorf("%w: %w", forge.ErrUnauthorized, pe)
		}
	}
	return err
}
