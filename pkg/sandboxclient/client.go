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

package sandboxclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gke-labs/eval-runner/pkg/discovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	processv1 "sigs.k8s.io/agent-sandbox/packages/sandboxd/spec/process/v1"
)

const (
	// DefaultGRPCPort is the default port for sandboxd gRPC ProcessService.
	DefaultGRPCPort = 9090

	// DefaultRESTPort is the default port for sandboxd REST FilesystemService.
	DefaultRESTPort = 8080

	// DefaultHTTPTimeout is the default timeout for REST HTTP operations.
	DefaultHTTPTimeout = 30 * time.Second

	// DefaultReadFileLimit is the default maximum size for files read via REST (10 MiB).
	DefaultReadFileLimit = 10 * 1024 * 1024
)

// ProcessConfig configures a process execution in the sandbox.
type ProcessConfig struct {
	Command []string
	Cwd     string
	Env     map[string]string
}

// ExecuteResult holds the outcome of a process execution.
type ExecuteResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
}

// Client interacts with a sandbox pod running sandboxd.
type Client interface {
	// Execute synchronously runs a command in the sandbox.
	Execute(ctx context.Context, cfg ProcessConfig) (*ExecuteResult, error)

	// ReadFile retrieves a file from the sandbox filesystem via REST.
	ReadFile(ctx context.Context, path string) ([]byte, error)

	// Close releases any underlying network connections.
	Close() error
}

// Factory creates Client instances for sandbox pod IPs.
type Factory interface {
	NewClient(podIP string, opts ...Option) (Client, error)
}

// FactoryFunc adapts a function into a Factory.
type FactoryFunc func(podIP string, opts ...Option) (Client, error)

// NewClient implements Factory.
func (f FactoryFunc) NewClient(podIP string, opts ...Option) (Client, error) {
	return f(podIP, opts...)
}

// ClientOptions configures a Client instance.
type ClientOptions struct {
	GRPCPort      int
	RESTPort      int
	StdoutLimit   int64
	StderrLimit   int64
	ReadFileLimit int64
	AuthToken     string
	TLSConfig     *tls.Config
	HTTPClient    *http.Client
}

// Option configures ClientOptions.
type Option func(*ClientOptions)

// WithTLSConfig sets the TLS configuration for connecting to sandboxd over mTLS.
func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(o *ClientOptions) {
		o.TLSConfig = tlsConfig
	}
}

// WithGRPCPort overrides the gRPC port.
func WithGRPCPort(port int) Option {
	return func(o *ClientOptions) {
		o.GRPCPort = port
	}
}

// WithRESTPort overrides the REST port.
func WithRESTPort(port int) Option {
	return func(o *ClientOptions) {
		o.RESTPort = port
	}
}

// WithStdoutLimit overrides the stdout limit.
func WithStdoutLimit(limit int64) Option {
	return func(o *ClientOptions) {
		o.StdoutLimit = limit
	}
}

// WithStderrLimit overrides the stderr limit.
func WithStderrLimit(limit int64) Option {
	return func(o *ClientOptions) {
		o.StderrLimit = limit
	}
}

// WithReadFileLimit overrides the file read limit.
func WithReadFileLimit(limit int64) Option {
	return func(o *ClientOptions) {
		o.ReadFileLimit = limit
	}
}

// WithAuthToken sets the bearer token for authenticating with sandboxd.
func WithAuthToken(token string) Option {
	return func(o *ClientOptions) {
		o.AuthToken = token
	}
}

// WithHTTPClient overrides the HTTP client.
func WithHTTPClient(client *http.Client) Option {
	return func(o *ClientOptions) {
		o.HTTPClient = client
	}
}

type sandboxdClient struct {
	grpcConn      *grpc.ClientConn
	processClient processv1.ProcessServiceClient
	httpClient    *http.Client
	restBaseURL   string
	stdoutLimit   int64
	stderrLimit   int64
	readFileLimit int64
	authToken     string
}

// DefaultFactory returns a Factory that constructs sandboxd clients with the provided default options.
func DefaultFactory(defaultOpts ...Option) Factory {
	return FactoryFunc(func(podIP string, opts ...Option) (Client, error) {
		combined := make([]Option, 0, len(defaultOpts)+len(opts))
		combined = append(combined, defaultOpts...)
		combined = append(combined, opts...)
		return New(podIP, combined...)
	})
}

// New creates a new Client connected to sandboxd on the given pod IP.
func New(podIP string, opts ...Option) (Client, error) {
	if podIP == "" {
		return nil, fmt.Errorf("pod IP is required")
	}

	options := ClientOptions{
		GRPCPort:      DefaultGRPCPort,
		RESTPort:      DefaultRESTPort,
		StdoutLimit:   discovery.MaxStdoutBytes,
		StderrLimit:   discovery.MaxStderrBytes,
		ReadFileLimit: DefaultReadFileLimit,
	}
	for _, opt := range opts {
		opt(&options)
	}

	var dialOpts []grpc.DialOption
	if options.TLSConfig != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(options.TLSConfig)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if options.AuthToken != "" {
		token := options.AuthToken
		dialOpts = append(dialOpts,
			grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
				ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
				return invoker(ctx, method, req, reply, cc, callOpts...)
			}),
			grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, callOpts ...grpc.CallOption) (grpc.ClientStream, error) {
				ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
				return streamer(ctx, desc, cc, method, callOpts...)
			}),
		)
	}

	grpcTarget := net.JoinHostPort(podIP, fmt.Sprintf("%d", options.GRPCPort))
	conn, err := grpc.NewClient(
		grpcTarget,
		dialOpts...,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client for %s: %w", grpcTarget, err)
	}

	httpClient := options.HTTPClient
	if httpClient == nil {
		var transport http.RoundTripper
		if options.TLSConfig != nil {
			t := http.DefaultTransport.(*http.Transport).Clone()
			t.TLSClientConfig = options.TLSConfig
			transport = t
		}
		httpClient = &http.Client{
			Transport: transport,
			Timeout:   DefaultHTTPTimeout,
		}
	}

	scheme := "http"
	if options.TLSConfig != nil {
		scheme = "https"
	}
	restBaseURL := fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(podIP, fmt.Sprintf("%d", options.RESTPort)))

	return &sandboxdClient{
		grpcConn:      conn,
		processClient: processv1.NewProcessServiceClient(conn),
		httpClient:    httpClient,
		restBaseURL:   restBaseURL,
		stdoutLimit:   options.StdoutLimit,
		stderrLimit:   options.StderrLimit,
		readFileLimit: options.ReadFileLimit,
		authToken:     options.AuthToken,
	}, nil
}

func (c *sandboxdClient) Execute(ctx context.Context, cfg ProcessConfig) (*ExecuteResult, error) {
	req := &processv1.ExecuteRequest{
		Config: &processv1.ProcessConfig{
			Command: cfg.Command,
			EnvVars: cfg.Env,
		},
	}
	if cfg.Cwd != "" {
		req.Config.Cwd = &cfg.Cwd
	}

	resp, err := c.processClient.Execute(ctx, req)
	if err != nil {
		return nil, err
	}

	stdoutLimit := c.stdoutLimit
	if stdoutLimit <= 0 {
		stdoutLimit = discovery.MaxStdoutBytes
	}
	stderrLimit := c.stderrLimit
	if stderrLimit <= 0 {
		stderrLimit = discovery.MaxStderrBytes
	}

	stdoutWriter := discovery.NewLimitWriter(stdoutLimit, "stdout")
	stderrWriter := discovery.NewLimitWriter(stderrLimit, "stderr")

	_, stdoutErr := stdoutWriter.Write(resp.GetStdout())
	_, stderrErr := stderrWriter.Write(resp.GetStderr())

	result := &ExecuteResult{
		ExitCode: int(resp.GetExitCode()),
		Stdout:   stdoutWriter.String(),
		Stderr:   stderrWriter.String(),
	}

	if stdoutErr != nil {
		return result, stdoutErr
	}
	if stderrErr != nil {
		return result, stderrErr
	}

	return result, nil
}

func (c *sandboxdClient) ReadFile(ctx context.Context, path string) ([]byte, error) {
	cleanPath := strings.TrimPrefix(path, "/")
	reqURL := fmt.Sprintf("%s/v1/files/%s", c.restBaseURL, cleanPath)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}
	if c.authToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.authToken)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to execute HTTP GET for %s: %w", path, err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			_ = closeErr
		}
	}()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("file %q not found (HTTP 404)", path)
	}

	limit := c.readFileLimit
	if limit <= 0 {
		limit = DefaultReadFileLimit
	}

	limitedReader := io.LimitReader(resp.Body, limit+1)
	body, err := io.ReadAll(limitedReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body for %s: %w", path, err)
	}

	if int64(len(body)) > limit {
		return nil, &discovery.LimitError{
			Stream: "file",
			Limit:  limit,
		}
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP GET %s failed with status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return body, nil
}

func (c *sandboxdClient) Close() error {
	if c.grpcConn != nil {
		return c.grpcConn.Close()
	}
	return nil
}
