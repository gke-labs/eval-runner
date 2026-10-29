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

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/pkg/controller"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func TestNewManagerOptions(t *testing.T) {
	tests := []struct {
		name        string
		leaderElect bool
	}{
		{
			name:        "leader election enabled",
			leaderElect: true,
		},
		{
			name:        "leader election disabled",
			leaderElect: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := newManagerOptions(scheme, cache.Options{}, tc.leaderElect, "eval-runner", ":8081")

			if opts.LeaderElection != tc.leaderElect {
				t.Errorf("LeaderElection = %v, want %v", opts.LeaderElection, tc.leaderElect)
			}
			if opts.LeaderElectionID != defaultLeaderElectionID {
				t.Errorf("LeaderElectionID = %q, want %q", opts.LeaderElectionID, defaultLeaderElectionID)
			}
			if opts.LeaderElectionNamespace != "eval-runner" {
				t.Errorf("LeaderElectionNamespace = %q, want %q", opts.LeaderElectionNamespace, "eval-runner")
			}
			if !opts.LeaderElectionReleaseOnCancel {
				t.Errorf("LeaderElectionReleaseOnCancel = false, want true")
			}
			if opts.LeaseDuration == nil || *opts.LeaseDuration != 60*time.Second {
				t.Errorf("LeaseDuration = %v, want %v", opts.LeaseDuration, 60*time.Second)
			}
			if opts.RenewDeadline == nil || *opts.RenewDeadline != 40*time.Second {
				t.Errorf("RenewDeadline = %v, want %v", opts.RenewDeadline, 40*time.Second)
			}
			if opts.RetryPeriod == nil || *opts.RetryPeriod != 8*time.Second {
				t.Errorf("RetryPeriod = %v, want %v", opts.RetryPeriod, 8*time.Second)
			}
			if opts.HealthProbeBindAddress != ":8081" {
				t.Errorf("HealthProbeBindAddress = %q, want %q", opts.HealthProbeBindAddress, ":8081")
			}
		})
	}
}

func TestRetryStartup_SuccessFirstAttempt(t *testing.T) {
	var logBuf bytes.Buffer
	logger := zap.New(zap.WriteTo(&logBuf), zap.UseDevMode(true))

	testBackoff := wait.Backoff{
		Duration: time.Millisecond,
		Factor:   1.0,
		Steps:    5,
	}

	calls := 0
	err := retryStartup(t.Context(), testBackoff, logger, "test operation", func(attemptCtx context.Context) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
	if logBuf.Len() > 0 {
		t.Errorf("expected no logs on immediate success, got: %s", logBuf.String())
	}
}

func TestRetryStartup_PodLookupRetryThenSuccess(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "eval-runner-controller-abc",
			Namespace: "eval-runner",
		},
	}
	fakeClient := k8sfake.NewSimpleClientset(pod)

	attempts := 0
	fakeClient.PrependReactor("get", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		attempts++
		if attempts <= 3 {
			return true, nil, fmt.Errorf("dial tcp 10.1.0.1:443: connect: connection refused")
		}
		return false, nil, nil
	})

	var logBuf bytes.Buffer
	logger := zap.New(zap.WriteTo(&logBuf), zap.UseDevMode(true))

	testBackoff := wait.Backoff{
		Duration: time.Millisecond,
		Factor:   1.0,
		Steps:    5,
	}

	var gotPod *corev1.Pod
	err := retryStartup(t.Context(), testBackoff, logger, "get controller pod", func(attemptCtx context.Context) error {
		var getErr error
		gotPod, getErr = fakeClient.CoreV1().Pods("eval-runner").Get(attemptCtx, "eval-runner-controller-abc", metav1.GetOptions{})
		return getErr
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotPod == nil || gotPod.Name != "eval-runner-controller-abc" {
		t.Fatalf("gotPod = %v, want pod with name eval-runner-controller-abc", gotPod)
	}
	if attempts != 4 {
		t.Errorf("attempts = %d, want 4", attempts)
	}

	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "unable to get controller pod; retrying") {
		t.Errorf("expected retry log in output, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "connection refused") {
		t.Errorf("expected connection refused in log output, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, `"attempt": 1`) || !strings.Contains(logOutput, `"attempt": 2`) || !strings.Contains(logOutput, `"attempt": 3`) {
		t.Errorf("expected attempt numbers in log output, got: %s", logOutput)
	}
	retryCount := strings.Count(logOutput, "unable to get controller pod; retrying")
	if retryCount != 3 {
		t.Errorf("expected 3 retry logs, got %d in: %s", retryCount, logOutput)
	}
}

type failingConfigReader struct {
	failCount int
	calls     int
	configMap *corev1.ConfigMap
}

func (r *failingConfigReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	r.calls++
	if r.calls <= r.failCount {
		return fmt.Errorf("dial tcp 10.1.0.1:443: connect: connection refused")
	}
	if r.configMap != nil && key.Name == r.configMap.Name && key.Namespace == r.configMap.Namespace {
		if cm, ok := obj.(*corev1.ConfigMap); ok {
			*cm = *r.configMap
			return nil
		}
	}
	return apierrors.NewNotFound(corev1.Resource("configmaps"), key.Name)
}

func (r *failingConfigReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return nil
}

func TestRetryStartup_ConfigLoadRetryThenSuccess(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      controller.DefaultConfigMapName,
			Namespace: "eval-runner",
		},
		Data: map[string]string{
			"discoveryDeadline": "5m",
		},
	}
	reader := &failingConfigReader{
		failCount: 3,
		configMap: cm,
	}

	var logBuf bytes.Buffer
	logger := zap.New(zap.WriteTo(&logBuf), zap.UseDevMode(true))

	testBackoff := wait.Backoff{
		Duration: time.Millisecond,
		Factor:   1.0,
		Steps:    5,
	}

	var clusterConfig controller.ClusterConfig
	err := retryStartup(t.Context(), testBackoff, logger, "load cluster configuration", func(attemptCtx context.Context) error {
		var loadErr error
		clusterConfig, loadErr = controller.LoadAndResolveClusterConfig(
			attemptCtx,
			reader,
			"eval-runner",
			controller.DefaultConfigMapName,
			controller.RawFlags{},
			logger,
		)
		return loadErr
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if clusterConfig.DiscoveryDeadline != 5*time.Minute {
		t.Errorf("clusterConfig.DiscoveryDeadline = %v, want 5m", clusterConfig.DiscoveryDeadline)
	}
	if reader.calls != 4 {
		t.Errorf("reader.calls = %d, want 4", reader.calls)
	}

	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "unable to load cluster configuration; retrying") {
		t.Errorf("expected retry log in output, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "connection refused") {
		t.Errorf("expected connection refused in log output, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, `"attempt": 1`) || !strings.Contains(logOutput, `"attempt": 2`) || !strings.Contains(logOutput, `"attempt": 3`) {
		t.Errorf("expected attempt numbers in log output, got: %s", logOutput)
	}
	retryCount := strings.Count(logOutput, "unable to load cluster configuration; retrying")
	if retryCount != 3 {
		t.Errorf("expected 3 retry logs, got %d in: %s", retryCount, logOutput)
	}
}

func TestRetryStartup_ExhaustedRetries(t *testing.T) {
	attempts := 0
	expectedErr := fmt.Errorf("dial tcp 10.1.0.1:443: connect: connection refused")

	var logBuf bytes.Buffer
	logger := zap.New(zap.WriteTo(&logBuf), zap.UseDevMode(true))

	testBackoff := wait.Backoff{
		Duration: time.Millisecond,
		Factor:   1.0,
		Steps:    3,
	}

	err := retryStartup(t.Context(), testBackoff, logger, "test operation", func(attemptCtx context.Context) error {
		attempts++
		return expectedErr
	})
	if err == nil {
		t.Fatalf("expected error after exhausted retries, got nil")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("expected connection refused error, got: %v", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}

	logOutput := logBuf.String()
	retryCount := strings.Count(logOutput, "unable to test operation; retrying")
	if retryCount != 3 {
		t.Errorf("expected 3 retry logs, got %d in: %s", retryCount, logOutput)
	}
	if !strings.Contains(logOutput, `"attempt": 1`) || !strings.Contains(logOutput, `"attempt": 2`) || !strings.Contains(logOutput, `"attempt": 3`) {
		t.Errorf("expected attempt numbers in log output, got: %s", logOutput)
	}
}

func TestRetryStartup_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // canceled before execution

	var logBuf bytes.Buffer
	logger := zap.New(zap.WriteTo(&logBuf), zap.UseDevMode(true))

	testBackoff := wait.Backoff{
		Duration: time.Millisecond,
		Factor:   1.0,
		Steps:    3,
	}

	calls := 0
	err := retryStartup(ctx, testBackoff, logger, "test operation", func(attemptCtx context.Context) error {
		calls++
		return fmt.Errorf("should not run")
	})
	if err == nil {
		t.Fatalf("expected error on canceled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got: %v", err)
	}
	if calls != 0 {
		t.Errorf("calls = %d, want 0", calls)
	}
}

func TestRetryStartup_AttemptTimeout(t *testing.T) {
	logger := zap.New(zap.UseDevMode(true))
	testBackoff := wait.Backoff{
		Duration: time.Millisecond,
		Steps:    1,
	}

	var deadlineSet bool
	var timeoutRemaining time.Duration
	err := retryStartup(t.Context(), testBackoff, logger, "test operation", func(attemptCtx context.Context) error {
		deadline, ok := attemptCtx.Deadline()
		if ok {
			deadlineSet = true
			timeoutRemaining = time.Until(deadline)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !deadlineSet {
		t.Errorf("expected deadline to be set on attemptCtx")
	}
	if timeoutRemaining < 5*time.Second || timeoutRemaining > defaultStartupAttemptTimeout+time.Second {
		t.Errorf("timeoutRemaining = %v, want approx %v", timeoutRemaining, defaultStartupAttemptTimeout)
	}
}

func TestRetryStartup_NilFn(t *testing.T) {
	logger := zap.New(zap.UseDevMode(true))
	testBackoff := wait.Backoff{Steps: 1}

	err := retryStartup(t.Context(), testBackoff, logger, "test operation", nil)
	if err == nil {
		t.Fatalf("expected error for nil fn, got nil")
	}
}

func TestDefaultStartupBackoff(t *testing.T) {
	if defaultStartupBackoff.Steps != 8 {
		t.Errorf("defaultStartupBackoff.Steps = %d, want 8", defaultStartupBackoff.Steps)
	}
	if defaultStartupBackoff.Duration != 1*time.Second {
		t.Errorf("defaultStartupBackoff.Duration = %v, want 1s", defaultStartupBackoff.Duration)
	}
	if defaultStartupBackoff.Factor != 2.0 {
		t.Errorf("defaultStartupBackoff.Factor = %v, want 2.0", defaultStartupBackoff.Factor)
	}
	if defaultStartupBackoff.Jitter != 0.1 {
		t.Errorf("defaultStartupBackoff.Jitter = %v, want 0.1", defaultStartupBackoff.Jitter)
	}
	if defaultStartupRetryTimeout != 2*time.Minute {
		t.Errorf("defaultStartupRetryTimeout = %v, want 2m", defaultStartupRetryTimeout)
	}
	if defaultStartupAttemptTimeout != 10*time.Second {
		t.Errorf("defaultStartupAttemptTimeout = %v, want 10s", defaultStartupAttemptTimeout)
	}
}
