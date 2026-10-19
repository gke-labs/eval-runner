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
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
	"github.com/gke-labs/eval-runner/pkg/artifacts/gcs"
	"github.com/gke-labs/eval-runner/pkg/controller"
	"github.com/gke-labs/eval-runner/pkg/discovery"
	"github.com/gke-labs/eval-runner/pkg/pki"
	"github.com/gke-labs/eval-runner/pkg/sandboxclient"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	discoveryk8s "k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	utilruntime.Must(sandboxv1beta1.AddToScheme(scheme))
}

func checkSandboxCRD(discoveryClient discoveryk8s.DiscoveryInterface) error {
	resources, err := discoveryClient.ServerResourcesForGroupVersion("agents.x-k8s.io/v1beta1")
	if err != nil {
		return fmt.Errorf("groupVersion agents.x-k8s.io/v1beta1 not found: %w", err)
	}
	for _, r := range resources.APIResources {
		if r.Kind == "Sandbox" {
			return nil
		}
	}
	return fmt.Errorf("resource kind Sandbox not found in agents.x-k8s.io/v1beta1")
}

func main() {
	var evalRunnerNamespace string
	var sandboxNamespace string
	var probeAddr string
	var discoveryDeadline time.Duration
	var executionDeadline time.Duration
	var taskGracePeriod time.Duration
	var maxStdoutBytes int64
	var maxStderrBytes int64
	var maxDiscoveryAttempts int
	var maxParallelTasks int
	var modelSecret string
	var sampleRunnerImage string
	var sandboxdImage string
	var artifactBucket string
	var artifactDir string
	var artifactMaxUploadBytes int64
	var gitHubTokenSecret string
	var gitHubAppSecret string
	var uiBaseURL string
	var inconclusivePolicy string
	var strictConfig bool
	var comparisonTTL time.Duration

	flag.StringVar(&evalRunnerNamespace, "namespace", discovery.DefaultControllerNamespace, "Namespace in which the controller operates")
	flag.StringVar(&sandboxNamespace, "sandbox-namespace", discovery.DefaultSandboxNamespace, "Namespace in which sandboxes are spawned")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.DurationVar(&discoveryDeadline, "discovery-deadline", controller.DefaultDiscoveryDeadline, "Overall deadline for discovery to complete")
	flag.DurationVar(&executionDeadline, "execution-deadline", controller.DefaultExecutionDeadline, "Overall deadline for sample execution to complete")
	flag.DurationVar(&taskGracePeriod, "task-grace-period", controller.DefaultTaskGracePeriod, "Grace period before failing a Running task whose sandbox is missing")
	flag.Int64Var(&maxStdoutBytes, "max-stdout-bytes", discovery.MaxStdoutBytes, "Maximum stdout bytes buffered from discovery execution")
	flag.Int64Var(&maxStderrBytes, "max-stderr-bytes", discovery.MaxStderrBytes, "Maximum stderr bytes buffered from discovery execution")
	flag.IntVar(&maxDiscoveryAttempts, "max-discovery-attempts", controller.DefaultMaxDiscoveryAttempts, "Maximum number of git clone retry attempts on failure")
	flag.IntVar(&maxParallelTasks, "max-parallel-tasks", controller.DefaultMaxParallelTasks, "Maximum number of task sandboxes to run concurrently")
	flag.StringVar(&modelSecret, "model-secret", controller.DefaultModelSecret, "Name of Secret in sandbox namespace containing model credentials")
	flag.StringVar(&gitHubTokenSecret, "github-token-secret", controller.DefaultGitHubTokenSecret, "Name of Secret in controller namespace containing GitHub token")
	flag.StringVar(&gitHubAppSecret, "github-app-secret", "", "Name of Secret in controller namespace containing GitHub App credentials")
	flag.StringVar(&uiBaseURL, "ui-base-url", "", "Base URL for the eval-runner UI (e.g. https://eval-runner.example.com)")
	flag.StringVar(&inconclusivePolicy, "inconclusive-policy", controller.DefaultInconclusivePolicy, "Check-run conclusion policy for inconclusive verdicts: neutral (default) or failure")
	flag.StringVar(&sampleRunnerImage, "sample-runner-image", "", "Container image used for sample execution sandboxes (derived from controller pod image if unset)")
	flag.StringVar(&sandboxdImage, "sandboxd-image", "", "Container image used for sandboxd init container (defaults to pinned SandboxdImage if unset)")
	flag.StringVar(&artifactBucket, "artifact-bucket", "", "GCS bucket URI or name for storing evaluation artifacts (e.g. gs://my-bucket)")
	flag.StringVar(&artifactDir, "artifact-dir", "", "Local filesystem directory for storing evaluation artifacts")
	flag.BoolVar(&strictConfig, "strict-config", false, "Fail startup if eval-runner-config contains unrecognised keys")
	flag.Int64Var(&artifactMaxUploadBytes, "artifact-max-upload-bytes", gcs.DefaultMaxUploadBytes, "Maximum size in bytes of an uploaded Inspect log before truncation (sets capped in summary.json if exceeded)")
	flag.DurationVar(&comparisonTTL, "comparison-ttl", 0, "TTL duration for retaining Completed Comparisons (default 0 = disabled)")

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	rawFlags := controller.FlagsFromFlagSet(
		flag.CommandLine,
		artifactBucket,
		artifactDir,
		modelSecret,
		sandboxdImage,
		gitHubTokenSecret,
		gitHubAppSecret,
		uiBaseURL,
		discoveryDeadline,
		executionDeadline,
		taskGracePeriod,
		maxParallelTasks,
		strictConfig,
		artifactMaxUploadBytes,
		inconclusivePolicy,
		comparisonTTL,
	)

	cfg := ctrl.GetConfigOrDie()

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		setupLog.Error(err, "unable to create kubernetes client")
		os.Exit(1)
	}

	if sampleRunnerImage == "" {
		podName := os.Getenv("POD_NAME")
		podNamespace := os.Getenv("POD_NAMESPACE")
		if podName == "" || podNamespace == "" {
			setupLog.Error(fmt.Errorf("POD_NAME and POD_NAMESPACE environment variables are required when --sample-runner-image is unset"), "cannot derive sample-runner image from controller pod")
			os.Exit(1)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		pod, err := kubeClient.CoreV1().Pods(podNamespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			setupLog.Error(err, "unable to get controller pod to derive sample-runner image", "pod", podName, "namespace", podNamespace)
			os.Exit(1)
		}

		controllerImage, err := controller.FindEvalRunnerImage(pod)
		if err != nil {
			setupLog.Error(err, "unable to find eval-runner container in controller pod", "pod", podName, "namespace", podNamespace)
			os.Exit(1)
		}

		derivedImage, err := controller.DeriveSampleRunnerImage(controllerImage)
		if err != nil {
			setupLog.Error(err, "failed to derive sample-runner image from controller image", "controllerImage", controllerImage)
			os.Exit(1)
		}

		sampleRunnerImage = derivedImage
		setupLog.Info("derived sample-runner image from controller image", "image", sampleRunnerImage, "controllerImage", controllerImage)
	} else {
		setupLog.Info("using configured sample-runner image override", "image", sampleRunnerImage)
	}

	// Pre-requisite check: Fail fast if agent-sandbox CRD is missing
	discoveryClient, err := discoveryk8s.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		setupLog.Error(err, "unable to create discovery client")
		os.Exit(1)
	}

	if err := checkSandboxCRD(discoveryClient); err != nil {
		setupLog.Error(err, "agent-sandbox CRD (agents.x-k8s.io/v1beta1) is missing; agent-sandbox must be installed as a prerequisite")
		os.Exit(1)
	}

	// Least-privilege namespaced caching per object:
	// Runs and Comparisons are only cached in evalRunnerNamespace.
	// Sandboxes and Pods are only cached in sandboxNamespace.
	cacheOptions := cache.Options{
		ByObject: map[client.Object]cache.ByObject{
			&v1alpha1.Run{}: {
				Namespaces: map[string]cache.Config{
					evalRunnerNamespace: {},
				},
			},
			&v1alpha1.Comparison{}: {
				Namespaces: map[string]cache.Config{
					evalRunnerNamespace: {},
				},
			},
			&sandboxv1beta1.Sandbox{}: {
				Namespaces: map[string]cache.Config{
					sandboxNamespace: {},
				},
			},
			&corev1.Pod{}: {
				Namespaces: map[string]cache.Config{
					sandboxNamespace: {},
				},
			},
		},
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		Cache:  cacheOptions,
		Client: client.Options{
			Cache: &client.CacheOptions{
				DisableFor: []client.Object{
					&corev1.Secret{},
					&corev1.ConfigMap{},
				},
			},
		},
		LeaderElection:         false,
		HealthProbeBindAddress: probeAddr,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	cfgCtx, cfgCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cfgCancel()

	clusterConfig, err := controller.LoadAndResolveClusterConfig(
		cfgCtx,
		mgr.GetAPIReader(),
		evalRunnerNamespace,
		controller.DefaultConfigMapName,
		rawFlags,
		setupLog,
	)
	if err != nil {
		setupLog.Error(err, "failed to load cluster configuration", "configmap", controller.DefaultConfigMapName, "namespace", evalRunnerNamespace)
		os.Exit(1)
	}

	// Operational visibility check for model credentials secret at startup
	controller.CheckModelSecretAtStartup(context.Background(), kubeClient, sandboxNamespace, clusterConfig.ModelSecret, setupLog)

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	clientFactory := sandboxclient.DefaultFactory(
		sandboxclient.WithStdoutLimit(maxStdoutBytes),
		sandboxclient.WithStderrLimit(maxStderrBytes),
	)

	pkiManager := pki.NewManager(mgr.GetClient(), evalRunnerNamespace, sandboxNamespace)
	pkiCtx, pkiCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer pkiCancel()
	if _, err := pkiManager.EnsureCA(pkiCtx); err != nil {
		setupLog.Error(err, "unable to bootstrap sandbox CA")
		os.Exit(1)
	}

	var sink artifacts.ArtifactSink
	if clusterConfig.ArtifactBucket != "" {
		var gcsOpts []gcs.Option
		if clusterConfig.ArtifactMaxUploadBytes > 0 {
			gcsOpts = append(gcsOpts, gcs.WithMaxUploadBytes(clusterConfig.ArtifactMaxUploadBytes))
		}
		gcsSink, err := gcs.New(context.Background(), clusterConfig.ArtifactBucket, gcsOpts...)
		if err != nil {
			setupLog.Error(err, "unable to configure GCS artifact sink", "bucket", clusterConfig.ArtifactBucket)
			os.Exit(1)
		}
		sink = gcsSink
	} else if clusterConfig.ArtifactDir != "" {
		sink = &artifacts.LocalDirSink{BaseDir: clusterConfig.ArtifactDir}
	} else {
		setupLog.Info("neither artifactBucket nor artifactDir configured; using default in-memory sink (non-durable: controller restarts reset Runs to Pending to re-discover)")
		sink = artifacts.NewInMemorySink()
	}
	defer func() {
		if err := sink.Close(); err != nil {
			setupLog.Error(err, "error closing artifact sink on shutdown")
		}
	}()

	reconciler := &controller.RunReconciler{
		Client:               mgr.GetClient(),
		APIReader:            mgr.GetAPIReader(),
		Scheme:               mgr.GetScheme(),
		ClientFactory:        clientFactory,
		ControllerNamespace:  evalRunnerNamespace,
		SandboxNamespace:     sandboxNamespace,
		PKIManager:           pkiManager,
		SandboxdImage:        clusterConfig.SandboxdImage,
		DiscoveryDeadline:    clusterConfig.DiscoveryDeadline,
		ExecutionDeadline:    clusterConfig.ExecutionDeadline,
		TaskGracePeriod:      clusterConfig.TaskGracePeriod,
		MaxMessageBytes:      discovery.MaxMessageBytes,
		MaxDiscoveryAttempts: maxDiscoveryAttempts,
		MaxParallelTasks:     clusterConfig.MaxParallelTasks,
		ModelSecret:          clusterConfig.ModelSecret,
		SampleRunnerImage:    sampleRunnerImage,
		ArtifactSink:         sink,
	}

	if err = reconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Run")
		os.Exit(1)
	}

	comparisonReconciler := &controller.ComparisonReconciler{
		Client:              mgr.GetClient(),
		Scheme:              mgr.GetScheme(),
		ArtifactSink:        sink,
		ControllerNamespace: evalRunnerNamespace,
		GitHubTokenSecret:   clusterConfig.GitHubTokenSecret,
		GitHubAppSecret:     clusterConfig.GitHubAppSecret,
		UIBaseURL:           clusterConfig.UIBaseURL,
		InconclusivePolicy:  clusterConfig.InconclusivePolicy,
		ComparisonTTL:       clusterConfig.ComparisonTTL,
	}

	if err = comparisonReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Comparison")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
