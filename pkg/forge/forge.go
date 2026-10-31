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

package forge

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNotFound indicates that a repository, branch, change request, or commit was not found.
	ErrNotFound = errors.New("not found")

	// ErrUnsupported indicates that an operation is not supported by the forge.
	ErrUnsupported = errors.New("operation not supported")

	// ErrUnauthorized indicates that required authentication credentials are missing or invalid.
	ErrUnauthorized = errors.New("unauthorized: credentials required")
)

// A Forge is a source of truth about repositories and change requests.
type Forge interface {
	// Kind returns a stable identifier such as "github.com".
	Kind() string
	// Repository returns metadata for a repository reference (host/owner/name).
	Repository(ctx context.Context, ref RepoRef) (*Repository, error)
	// Branches lists branch heads (name -> commit sha).
	Branches(ctx context.Context, ref RepoRef) ([]Branch, error)
	// ChangeRequests lists change requests with head/base refs and shas.
	// Forges that discover change requests from refs without access to forge state APIs
	// (such as gitremote) report State: "unknown".
	ChangeRequests(ctx context.Context, ref RepoRef) ([]ChangeRequest, error)
	// Commit returns commit metadata (sha, parents, author time, message title).
	Commit(ctx context.Context, ref RepoRef, sha string) (*Commit, error)
	// Reporter is optional: a forge that can post results back implements it.
}

// Reporter is an optional interface implemented by forges that can post evaluation results back.
type Reporter interface {
	// UpsertComment posts or updates a comment on a change request identified by key.
	// It returns the platform comment ID if available.
	UpsertComment(ctx context.Context, ref RepoRef, id ChangeRequestID, key string, body string) (int64, error)
	// SetStatus reports a status check on a commit SHA.
	// It returns the platform status or check run ID if available.
	SetStatus(ctx context.Context, ref RepoRef, sha string, status Status) (int64, error)
}

// Repository describes metadata for a repository reference.
type Repository struct {
	Ref           RepoRef `json:"ref"`
	DefaultBranch string  `json:"default_branch"`
	CloneURL      string  `json:"clone_url"`
}

// Branch represents a named branch head in a repository.
type Branch struct {
	Name string `json:"name"`
	SHA  string `json:"sha"`
}

// ChangeRequestID uniquely identifies a change request within a repository.
type ChangeRequestID string

// ChangeRequestHead represents the head (source) of a change request.
type ChangeRequestHead struct {
	Ref      string  `json:"ref"`
	SHA      string  `json:"sha"`
	RepoRef  RepoRef `json:"repo_ref"`
	CloneURL string  `json:"clone_url,omitempty"`
}

// ChangeRequestBase represents the base (target) branch of a change request.
type ChangeRequestBase struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

// ChangeRequest represents an open change request (pull request / merge request).
type ChangeRequest struct {
	ID        ChangeRequestID   `json:"id"`
	Number    int               `json:"number"`
	Title     string            `json:"title"`
	Author    string            `json:"author"`
	Head      ChangeRequestHead `json:"head"`
	Base      ChangeRequestBase `json:"base"`
	State     string            `json:"state"`
	Merged    bool              `json:"merged,omitempty"`
	UpdatedAt time.Time         `json:"updated_at"`
	MergeSHA  string            `json:"merge_sha,omitempty"`
}

// Commit describes commit metadata.
type Commit struct {
	SHA     string    `json:"sha"`
	Parents []string  `json:"parents"`
	Time    time.Time `json:"time"`
	Title   string    `json:"title"`
}

// CheckRunImage represents an image attached to a check run.
type CheckRunImage struct {
	Alt      string `json:"alt"`
	ImageURL string `json:"image_url"`
	Caption  string `json:"caption,omitempty"`
}

// CheckRunAnnotation represents a file annotation on a check run.
type CheckRunAnnotation struct {
	Path            string `json:"path"`
	StartLine       int    `json:"start_line"`
	EndLine         int    `json:"end_line"`
	StartColumn     *int   `json:"start_column,omitempty"`
	EndColumn       *int   `json:"end_column,omitempty"`
	AnnotationLevel string `json:"annotation_level"` // "notice", "warning", "failure"
	Message         string `json:"message"`
	Title           string `json:"title,omitempty"`
	RawDetails      string `json:"raw_details,omitempty"`
}

// CheckRunAction represents an interactive action button on a check run.
type CheckRunAction struct {
	Label       string `json:"label"`
	Description string `json:"description"`
	Identifier  string `json:"identifier"`
}

// Status represents a commit status check or check run posted back to a forge.
type Status struct {
	Status      string               `json:"status,omitempty"`
	Conclusion  string               `json:"conclusion,omitempty"`
	State       string               `json:"state,omitempty"`
	Context     string               `json:"context"`
	Description string               `json:"description"`
	Text        string               `json:"text,omitempty"`
	TargetURL   string               `json:"target_url"`
	CheckRunID  int64                `json:"check_run_id,omitempty"`
	Images      []CheckRunImage      `json:"images,omitempty"`
	Annotations []CheckRunAnnotation `json:"annotations,omitempty"`
	Actions     []CheckRunAction     `json:"actions,omitempty"`
}
