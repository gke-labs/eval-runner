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
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/pkg/discovery"
	"github.com/gke-labs/eval-runner/pkg/pki"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	processv1 "sigs.k8s.io/agent-sandbox/packages/sandboxd/spec/process/v1"
)

type mockProcessServer struct {
	processv1.UnimplementedProcessServiceServer
	execResp *processv1.ExecuteResponse
	execErr  error
	lastReq  *processv1.ExecuteRequest
}

func (m *mockProcessServer) Execute(ctx context.Context, req *processv1.ExecuteRequest) (*processv1.ExecuteResponse, error) {
	m.lastReq = req
	if m.execErr != nil {
		return nil, m.execErr
	}
	return m.execResp, nil
}

func TestSandboxClient_ExecuteAndLimits(t *testing.T) {
	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer grpcLis.Close()

	mockSrv := &mockProcessServer{
		execResp: &processv1.ExecuteResponse{
			ExitCode: 0,
			Stdout:   []byte("hello world"),
			Stderr:   []byte("some warning"),
		},
	}
	grpcServer := grpc.NewServer()
	processv1.RegisterProcessServiceServer(grpcServer, mockSrv)
	go func() {
		_ = grpcServer.Serve(grpcLis)
	}()
	defer grpcServer.Stop()

	_, portStr, err := net.SplitHostPort(grpcLis.Addr().String())
	if err != nil {
		t.Fatalf("failed to split host port: %v", err)
	}
	grpcPort, _ := strconv.Atoi(portStr)

	client, err := New("127.0.0.1", WithGRPCPort(grpcPort))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer client.Close()

	res, err := client.Execute(t.Context(), ProcessConfig{
		Command: []string{"echo", "hi"},
		Cwd:     "repo",
	})
	if err != nil {
		t.Fatalf("Execute unexpected error: %v", err)
	}
	if res.ExitCode != 0 {
		t.Errorf("expected exit code 0, got %d", res.ExitCode)
	}
	if res.Stdout != "hello world" {
		t.Errorf("expected stdout 'hello world', got %q", res.Stdout)
	}
	if res.Stderr != "some warning" {
		t.Errorf("expected stderr 'some warning', got %q", res.Stderr)
	}
	if mockSrv.lastReq == nil || mockSrv.lastReq.Config.GetCwd() != "repo" {
		t.Errorf("expected Cwd 'repo', got: %+v", mockSrv.lastReq)
	}

	// Test stdout limit
	clientWithStdoutLimit, err := New("127.0.0.1", WithGRPCPort(grpcPort), WithStdoutLimit(5))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer clientWithStdoutLimit.Close()

	res, err = clientWithStdoutLimit.Execute(t.Context(), ProcessConfig{Command: []string{"test"}})
	if err == nil {
		t.Fatalf("expected stdout limit error, got nil")
	}
	var limitErr *discovery.LimitError
	if !errors.As(err, &limitErr) || limitErr.Stream != "stdout" {
		t.Fatalf("expected stdout LimitError, got: %v", err)
	}
	if res.Stdout != "hello" {
		t.Errorf("expected truncated stdout 'hello', got %q", res.Stdout)
	}

	// Test stderr limit
	clientWithStderrLimit, err := New("127.0.0.1", WithGRPCPort(grpcPort), WithStderrLimit(4))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer clientWithStderrLimit.Close()

	res, err = clientWithStderrLimit.Execute(t.Context(), ProcessConfig{Command: []string{"test"}})
	if err == nil {
		t.Fatalf("expected stderr limit error, got nil")
	}
	if !errors.As(err, &limitErr) || limitErr.Stream != "stderr" {
		t.Fatalf("expected stderr LimitError, got: %v", err)
	}
	if res.Stderr != "some" {
		t.Errorf("expected truncated stderr 'some', got %q", res.Stderr)
	}
}

func TestSandboxClient_ReadFile(t *testing.T) {
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/files/repo/output.json" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result": "ok"}`))
			return
		}
		if r.URL.Path == "/v1/files/missing.txt" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code": "NOT_FOUND"}`))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`server error`))
	}))
	defer httpServer.Close()

	_, portStr, err := net.SplitHostPort(httpServer.Listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to split host port: %v", err)
	}
	httpPort, _ := strconv.Atoi(portStr)

	client, err := New("127.0.0.1", WithRESTPort(httpPort), WithHTTPClient(httpServer.Client()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer client.Close()

	// Read existing file
	data, err := client.ReadFile(t.Context(), "repo/output.json")
	if err != nil {
		t.Fatalf("unexpected error reading file: %v", err)
	}
	if string(data) != `{"result": "ok"}` {
		t.Errorf("expected file content '{\"result\": \"ok\"}', got %q", string(data))
	}

	// Read missing file
	_, err = client.ReadFile(t.Context(), "missing.txt")
	if err == nil || !strings.Contains(err.Error(), "404") {
		t.Errorf("expected 404 error, got: %v", err)
	}

	// Read failing file
	_, err = client.ReadFile(t.Context(), "server-error.txt")
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Errorf("expected 500 error, got: %v", err)
	}

	// Test read file limit
	clientWithFileLimit, err := New("127.0.0.1", WithRESTPort(httpPort), WithHTTPClient(httpServer.Client()), WithReadFileLimit(5))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer clientWithFileLimit.Close()

	_, err = clientWithFileLimit.ReadFile(t.Context(), "repo/output.json")
	if err == nil {
		t.Fatalf("expected LimitError for file read over limit, got nil")
	}
	var limitErr *discovery.LimitError
	if !errors.As(err, &limitErr) || limitErr.Stream != "file" {
		t.Errorf("expected LimitError for file stream, got: %v", err)
	}
}

func TestSandboxClient_Validation(t *testing.T) {
	_, err := New("")
	if err == nil {
		t.Errorf("expected error when podIP is empty")
	}
}

func TestSandboxClient_BearerAuth(t *testing.T) {
	expectedToken := "correct-secret-bearer-token-12345"

	// Mock gRPC server that requires Bearer auth
	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer grpcLis.Close()

	authUnaryInterceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}
		auths := md.Get("authorization")
		if len(auths) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing authorization header")
		}
		if auths[0] != "Bearer "+expectedToken {
			return nil, status.Error(codes.Unauthenticated, "invalid bearer token")
		}
		return handler(ctx, req)
	}

	mockSrv := &mockProcessServer{
		execResp: &processv1.ExecuteResponse{
			ExitCode: 0,
			Stdout:   []byte("authenticated exec"),
		},
	}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(authUnaryInterceptor))
	processv1.RegisterProcessServiceServer(grpcServer, mockSrv)
	go func() {
		_ = grpcServer.Serve(grpcLis)
	}()
	defer grpcServer.Stop()

	_, portStr, err := net.SplitHostPort(grpcLis.Addr().String())
	if err != nil {
		t.Fatalf("failed to split host port: %v", err)
	}
	grpcPort, _ := strconv.Atoi(portStr)

	// Mock HTTP server that requires Bearer auth except for /v1/health
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/health" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status": "ok"}`))
			return
		}
		auth := r.Header.Get("Authorization")
		if auth != "Bearer "+expectedToken {
			w.Header().Set("WWW-Authenticate", `Bearer realm="sandboxd"`)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"code":"UNAUTHENTICATED","message":"missing or invalid bearer token"}`))
			return
		}
		if r.URL.Path == "/v1/files/repo/protected.txt" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`secret data`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer httpServer.Close()

	_, httpPortStr, err := net.SplitHostPort(httpServer.Listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to split host port: %v", err)
	}
	httpPort, _ := strconv.Atoi(httpPortStr)

	// 1. Unauthenticated client: call without token should be rejected
	unauthClient, err := New("127.0.0.1",
		WithGRPCPort(grpcPort),
		WithRESTPort(httpPort),
		WithHTTPClient(httpServer.Client()),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer unauthClient.Close()

	_, execErr := unauthClient.Execute(t.Context(), ProcessConfig{Command: []string{"echo"}})
	if execErr == nil {
		t.Fatalf("expected gRPC Execute to fail without auth token, but succeeded")
	}
	if st, ok := status.FromError(execErr); !ok || st.Code() != codes.Unauthenticated {
		t.Errorf("expected gRPC code Unauthenticated, got: %v", execErr)
	}

	_, readErr := unauthClient.ReadFile(t.Context(), "repo/protected.txt")
	if readErr == nil {
		t.Fatalf("expected REST ReadFile to fail without auth token, but succeeded")
	}
	if !strings.Contains(readErr.Error(), "401") && !strings.Contains(readErr.Error(), "UNAUTHENTICATED") {
		t.Errorf("expected 401/UNAUTHENTICATED error, got: %v", readErr)
	}

	// 2. Client with wrong token: should be rejected
	wrongTokenClient, err := New("127.0.0.1",
		WithGRPCPort(grpcPort),
		WithRESTPort(httpPort),
		WithHTTPClient(httpServer.Client()),
		WithAuthToken("bogus-token"),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer wrongTokenClient.Close()

	_, execErr = wrongTokenClient.Execute(t.Context(), ProcessConfig{Command: []string{"echo"}})
	if execErr == nil {
		t.Fatalf("expected gRPC Execute to fail with invalid auth token, but succeeded")
	}
	if st, ok := status.FromError(execErr); !ok || st.Code() != codes.Unauthenticated {
		t.Errorf("expected gRPC code Unauthenticated for wrong token, got: %v", execErr)
	}

	_, readErr = wrongTokenClient.ReadFile(t.Context(), "repo/protected.txt")
	if readErr == nil {
		t.Fatalf("expected REST ReadFile to fail with invalid auth token, but succeeded")
	}
	if !strings.Contains(readErr.Error(), "401") && !strings.Contains(readErr.Error(), "UNAUTHENTICATED") {
		t.Errorf("expected 401/UNAUTHENTICATED error for wrong token, got: %v", readErr)
	}

	// 3. Authenticated client: calls should succeed
	authClient, err := New("127.0.0.1",
		WithGRPCPort(grpcPort),
		WithRESTPort(httpPort),
		WithHTTPClient(httpServer.Client()),
		WithAuthToken(expectedToken),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer authClient.Close()

	execRes, err := authClient.Execute(t.Context(), ProcessConfig{Command: []string{"echo"}})
	if err != nil {
		t.Fatalf("expected gRPC Execute to succeed with valid token, got: %v", err)
	}
	if execRes.ExitCode != 0 || execRes.Stdout != "authenticated exec" {
		t.Errorf("unexpected execute result: %+v", execRes)
	}

	fileContent, err := authClient.ReadFile(t.Context(), "repo/protected.txt")
	if err != nil {
		t.Fatalf("expected REST ReadFile to succeed with valid token, got: %v", err)
	}
	if string(fileContent) != "secret data" {
		t.Errorf("expected file content 'secret data', got: %q", string(fileContent))
	}

	// 4. Factory creates authenticated client properly
	factory := DefaultFactory(
		WithGRPCPort(grpcPort),
		WithRESTPort(httpPort),
		WithHTTPClient(httpServer.Client()),
	)
	factoryClient, err := factory.NewClient("127.0.0.1", WithAuthToken(expectedToken))
	if err != nil {
		t.Fatalf("factory NewClient failed: %v", err)
	}
	defer factoryClient.Close()

	execRes, err = factoryClient.Execute(t.Context(), ProcessConfig{Command: []string{"echo"}})
	if err != nil {
		t.Fatalf("expected factory client Execute to succeed, got: %v", err)
	}
	if execRes.Stdout != "authenticated exec" {
		t.Errorf("unexpected execute result from factory client: %+v", execRes)
	}
}

func TestSandboxClient_TLSAndSANVerification(t *testing.T) {
	ca, err := pki.NewInMemoryCA()
	if err != nil {
		t.Fatalf("failed to create CA: %v", err)
	}

	sandboxName := "sandbox-a"
	sandboxNamespace := "eval-runner-sandbox"
	serverCertPEM, serverKeyPEM, err := ca.MintServerCert(sandboxName, sandboxNamespace, 1*time.Hour, net.ParseIP("127.0.0.1"))
	if err != nil {
		t.Fatalf("failed to mint server cert: %v", err)
	}
	serverTLS, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	if err != nil {
		t.Fatalf("failed to load server TLS: %v", err)
	}

	serverTLSConfig := &tls.Config{
		Certificates: []tls.Certificate{serverTLS},
		ClientCAs:    ca.CertPool(),
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}

	// 1. Start gRPC server with TLS
	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen gRPC: %v", err)
	}
	defer grpcLis.Close()

	mockSrv := &mockProcessServer{
		execResp: &processv1.ExecuteResponse{
			ExitCode: 0,
			Stdout:   []byte("tls exec ok"),
		},
	}
	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLSConfig)))
	processv1.RegisterProcessServiceServer(grpcServer, mockSrv)
	go func() {
		_ = grpcServer.Serve(grpcLis)
	}()
	defer grpcServer.Stop()

	_, grpcPortStr, _ := net.SplitHostPort(grpcLis.Addr().String())
	grpcPort, _ := strconv.Atoi(grpcPortStr)

	// 2. Start REST server with TLS
	restLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen REST: %v", err)
	}
	defer restLis.Close()

	restMux := http.NewServeMux()
	restMux.HandleFunc("GET /v1/files/hello.txt", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("tls file ok"))
	})
	httpServer := &http.Server{
		Handler:   restMux,
		TLSConfig: serverTLSConfig,
	}
	go func() {
		_ = httpServer.ServeTLS(restLis, "", "")
	}()
	defer httpServer.Close()

	_, restPortStr, _ := net.SplitHostPort(restLis.Addr().String())
	restPort, _ := strconv.Atoi(restPortStr)

	// Case A: Correct client TLS config (chains to CA, matches sandbox-a SAN, presents valid client cert)
	validTLSConfig := ca.ClientTLSConfig(sandboxName, sandboxNamespace)
	client, err := New("127.0.0.1",
		WithGRPCPort(grpcPort),
		WithRESTPort(restPort),
		WithTLSConfig(validTLSConfig),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	execRes, err := client.Execute(t.Context(), ProcessConfig{Command: []string{"echo"}})
	if err != nil {
		t.Fatalf("expected Execute to succeed over mTLS, got: %v", err)
	}
	if execRes.Stdout != "tls exec ok" {
		t.Errorf("unexpected stdout: %q", execRes.Stdout)
	}

	content, err := client.ReadFile(t.Context(), "hello.txt")
	if err != nil {
		t.Fatalf("expected ReadFile to succeed over mTLS, got: %v", err)
	}
	if string(content) != "tls file ok" {
		t.Errorf("unexpected content: %q", string(content))
	}

	// Case B: Client expects sandbox-b (SAN mismatch: server presents cert for sandbox-a)
	mismatchedTLSConfig := ca.ClientTLSConfig("sandbox-b", sandboxNamespace)
	mismatchedClient, err := New("127.0.0.1",
		WithGRPCPort(grpcPort),
		WithRESTPort(restPort),
		WithTLSConfig(mismatchedTLSConfig),
	)
	if err != nil {
		t.Fatalf("failed to create mismatched client: %v", err)
	}
	defer mismatchedClient.Close()

	_, err = mismatchedClient.Execute(t.Context(), ProcessConfig{Command: []string{"echo"}})
	if err == nil {
		t.Errorf("expected Execute to fail due to SAN mismatch, but it succeeded")
	}

	_, err = mismatchedClient.ReadFile(t.Context(), "hello.txt")
	if err == nil {
		t.Errorf("expected ReadFile to fail due to SAN mismatch, but it succeeded")
	}

	// Case C: Client without client cert (sandboxd requires client cert)
	noClientCertTLS := &tls.Config{
		RootCAs:    ca.CertPool(),
		ServerName: pki.ServerSAN(sandboxName, sandboxNamespace),
	}
	noCertClient, err := New("127.0.0.1",
		WithGRPCPort(grpcPort),
		WithRESTPort(restPort),
		WithTLSConfig(noClientCertTLS),
	)
	if err != nil {
		t.Fatalf("failed to create noCertClient: %v", err)
	}
	defer noCertClient.Close()

	_, err = noCertClient.Execute(t.Context(), ProcessConfig{Command: []string{"echo"}})
	if err == nil {
		t.Errorf("expected Execute without client cert to fail, but it succeeded")
	}

	_, err = noCertClient.ReadFile(t.Context(), "hello.txt")
	if err == nil {
		t.Errorf("expected ReadFile without client cert to fail, but it succeeded")
	}
}
