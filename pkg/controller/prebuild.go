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

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
	"github.com/gke-labs/eval-runner/pkg/discovery"
	"github.com/gke-labs/eval-runner/pkg/pki"
	"github.com/gke-labs/eval-runner/pkg/sandboxclient"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type prebuildReconcileResult struct {
	hasStatusUpdates     bool
	waitingForOldSandbox bool
	waitingInGracePeriod bool
	waitingForPod        bool
	pollingAsync         bool
	minGraceRemaining    time.Duration
}

type prebuildUpdate struct {
	phase         v1alpha1.PrebuildPhase
	startedAt     *metav1.Time
	finishedAt    *metav1.Time
	message       string
	executionID   string
	setStarted    bool
	setFinished   bool
	setExecution  bool
	clearFinished bool
}

type prebuildCleanupTarget struct {
	prebuildName string
	sandbox      *sandboxv1beta1.Sandbox
	podUID       types.UID
}

func initializePrebuilds(tasks []v1alpha1.TaskStatus, manifest *v1alpha1.Manifest) []v1alpha1.PrebuildStatus {
	if manifest == nil || len(tasks) == 0 {
		return nil
	}

	manifestTasksByID := make(map[string]v1alpha1.ManifestTask, len(manifest.Tasks))
	for _, manifestTask := range manifest.Tasks {
		manifestTasksByID[manifestTask.ID] = manifestTask
	}

	seenPrebuilds := make(map[string]struct{})
	var prebuildNames []string
	for _, task := range tasks {
		if task.Phase != v1alpha1.TaskPhasePending {
			continue
		}
		manifestTask, exists := manifestTasksByID[task.ID]
		if !exists || manifestTask.Prebuild == "" {
			continue
		}
		if _, alreadySeen := seenPrebuilds[manifestTask.Prebuild]; alreadySeen {
			continue
		}
		seenPrebuilds[manifestTask.Prebuild] = struct{}{}
		prebuildNames = append(prebuildNames, manifestTask.Prebuild)
	}
	if len(prebuildNames) == 0 {
		return nil
	}
	slices.Sort(prebuildNames)

	statuses := make([]v1alpha1.PrebuildStatus, 0, len(prebuildNames))
	for _, prebuildName := range prebuildNames {
		manifestPrebuild, exists := manifest.Prebuilds[prebuildName]
		if !exists {
			statuses = append(statuses, v1alpha1.PrebuildStatus{
				Name:    prebuildName,
				Phase:   v1alpha1.PrebuildPhaseFailed,
				Message: fmt.Sprintf("prebuild %q not found in manifest", prebuildName),
			})
			continue
		}
		if err := validatePrebuildExecutionSpec(prebuildName, manifestPrebuild); err != nil {
			statuses = append(statuses, v1alpha1.PrebuildStatus{
				Name:    prebuildName,
				Phase:   v1alpha1.PrebuildPhaseFailed,
				Message: err.Error(),
			})
			continue
		}
		statuses = append(statuses, v1alpha1.PrebuildStatus{
			Name:  prebuildName,
			Phase: v1alpha1.PrebuildPhasePending,
		})
	}
	return statuses
}

func validatePrebuildExecutionSpec(prebuildName string, prebuild v1alpha1.ManifestPrebuild) error {
	if strings.TrimSpace(prebuild.Image) == "" {
		return fmt.Errorf("prebuild %q has empty image", prebuildName)
	}
	if len(prebuild.Command) == 0 || strings.TrimSpace(prebuild.Command[0]) == "" {
		return fmt.Errorf("prebuild %q has empty command", prebuildName)
	}
	if prebuild.TimeoutSeconds < 0 {
		return fmt.Errorf("prebuild %q has invalid timeoutSeconds %d: must be >= 0", prebuildName, prebuild.TimeoutSeconds)
	}
	for _, manifestResource := range prebuild.Resources {
		switch manifestResource.Type {
		case v1alpha1.ResourceTypeSandbox, v1alpha1.ResourceTypeKindCluster:
		default:
			return fmt.Errorf("prebuild %q uses unsupported resource type %q", prebuildName, manifestResource.Type)
		}
	}
	return discovery.ValidatePrebuildResources(prebuildName, prebuild)
}

func prebuildRequiresKindCluster(prebuild v1alpha1.ManifestPrebuild) bool {
	for _, manifestResource := range prebuild.Resources {
		if manifestResource.Type == v1alpha1.ResourceTypeKindCluster {
			return true
		}
	}
	return false
}

func prebuildPhaseByName(prebuilds []v1alpha1.PrebuildStatus) map[string]v1alpha1.PrebuildPhase {
	phases := make(map[string]v1alpha1.PrebuildPhase, len(prebuilds))
	for _, prebuild := range prebuilds {
		phases[prebuild.Name] = prebuild.Phase
	}
	return phases
}

func taskPrebuildByID(manifest *v1alpha1.Manifest) map[string]string {
	if manifest == nil {
		return nil
	}
	prebuildsByTask := make(map[string]string)
	for _, manifestTask := range manifest.Tasks {
		if manifestTask.Prebuild != "" {
			prebuildsByTask[manifestTask.ID] = manifestTask.Prebuild
		}
	}
	return prebuildsByTask
}

func prebuildExecutionID(runName, prebuildName string) string {
	return discovery.PrebuildSandboxNameForRun(runName, prebuildName)
}

func (r *RunReconciler) effectivePrebuildTimeout(prebuild v1alpha1.ManifestPrebuild) time.Duration {
	if prebuild.TimeoutSeconds > 0 {
		return time.Duration(prebuild.TimeoutSeconds) * time.Second
	}
	return r.executionStepTimeout()
}

func (r *RunReconciler) ensurePrebuildAuthToken(ctx context.Context, run *v1alpha1.Run, prebuildName string, sandboxKey types.NamespacedName) (string, error) {
	if token, ok := r.getAuthToken(sandboxKey.String()); ok && token != "" {
		return token, nil
	}

	secretNamespace := run.Namespace
	if secretNamespace == "" {
		secretNamespace = discovery.DefaultControllerNamespace
	}
	secretName := discovery.PrebuildAuthSecretNameForRun(run.Name, prebuildName)
	secretKey := types.NamespacedName{Namespace: secretNamespace, Name: secretName}

	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}

	var secret corev1.Secret
	if err := reader.Get(ctx, secretKey, &secret); err == nil {
		if tokenBytes, ok := secret.Data["token"]; ok && len(tokenBytes) > 0 {
			token := string(tokenBytes)
			r.setAuthToken(sandboxKey.String(), token)
			return token, nil
		}
	} else if !apierrors.IsNotFound(err) {
		return "", fmt.Errorf("failed to get prebuild auth secret: %w", err)
	}

	token, err := generateAuthToken()
	if err != nil {
		return "", err
	}

	secret = corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: secretNamespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"token": []byte(token),
		},
	}
	if r.Scheme != nil {
		if err := controllerutil.SetControllerReference(run, &secret, r.Scheme); err != nil {
			return "", fmt.Errorf("failed to set controller reference on prebuild auth secret: %w", err)
		}
	}

	if err := r.Create(ctx, &secret); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return "", fmt.Errorf("failed to create prebuild auth secret: %w", err)
		}
		if getErr := reader.Get(ctx, secretKey, &secret); getErr != nil {
			return "", fmt.Errorf("failed to get existing prebuild auth secret: %w", getErr)
		}
		tokenBytes, ok := secret.Data["token"]
		if !ok || len(tokenBytes) == 0 {
			return "", fmt.Errorf("existing prebuild auth secret %s lacks a token", secretKey)
		}
		token = string(tokenBytes)
	}

	r.setAuthToken(sandboxKey.String(), token)
	return token, nil
}

func (r *RunReconciler) cleanupPrebuildAuthToken(ctx context.Context, runNamespace, runName, prebuildName string, sandboxKey types.NamespacedName) {
	r.deleteAuthToken(sandboxKey.String())
	if err := r.pki().DeleteServerCertSecret(ctx, sandboxKey.Name); err != nil {
		logger := log.FromContext(ctx)
		logger.V(1).Info("Failed to delete prebuild server cert secret", "sandbox", sandboxKey.Name, "error", err)
	}
	if runNamespace == "" {
		runNamespace = discovery.DefaultControllerNamespace
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      discovery.PrebuildAuthSecretNameForRun(runName, prebuildName),
			Namespace: runNamespace,
		},
	}
	if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
		logger := log.FromContext(ctx)
		logger.V(1).Info("Failed to delete prebuild auth secret", "secret", secret.Name, "error", err)
	}
}

func (r *RunReconciler) buildPrebuildSandbox(
	run *v1alpha1.Run,
	prebuildName string,
	manifestPrebuild v1alpha1.ManifestPrebuild,
	sandboxName string,
	authToken string,
) *sandboxv1beta1.Sandbox {
	var kindClusterLimits corev1.ResourceList
	requiresKindCluster := prebuildRequiresKindCluster(manifestPrebuild)
	if requiresKindCluster {
		kindClusterLimits = discovery.PrebuildKindClusterResourceLimits(manifestPrebuild)
	}

	return discovery.BuildPrebuildSandbox(run, prebuildName, discovery.TaskSandboxConfig{
		Namespace:              r.sandboxNamespace(),
		ControllerInstance:     r.InstanceID,
		Image:                  manifestPrebuild.Image,
		AuthTokenDigest:        discovery.AuthTokenDigest(authToken),
		ServerCertSecret:       pki.ServerSecretName(sandboxName),
		CAConfigMap:            pki.DefaultCAConfigMapName,
		SandboxdImage:          r.sandboxdImage(),
		CloneRepo:              run.Spec.Repo.URL != "",
		StepTimeoutSeconds:     int(r.executionStepTimeout().Seconds()),
		SandboxTask:            true,
		KindCluster:            requiresKindCluster,
		KindClusterLimits:      kindClusterLimits,
		Limits:                 discovery.PrebuildResourceLimits(manifestPrebuild),
		ServicePortalIntercept: r.ServicePortalClient != nil,
	})
}

func (r *RunReconciler) newPrebuildSandboxClient(
	ctx context.Context,
	run *v1alpha1.Run,
	prebuildName string,
	sandbox *sandboxv1beta1.Sandbox,
	pod *corev1.Pod,
) (sandboxclient.Client, error) {
	authToken, err := r.ensurePrebuildAuthToken(ctx, run, prebuildName, types.NamespacedName{Namespace: sandbox.Namespace, Name: sandbox.Name})
	if err != nil {
		return nil, fmt.Errorf("failed to get prebuild auth token: %w", err)
	}
	if r.SandboxClient != nil {
		return r.SandboxClient, nil
	}
	var options []sandboxclient.Option
	if authToken != "" {
		options = append(options, sandboxclient.WithAuthToken(authToken))
	}
	if tlsConfig := r.pki().ClientTLSConfig(sandbox.Name, sandbox.Namespace); tlsConfig != nil {
		options = append(options, sandboxclient.WithTLSConfig(tlsConfig))
	}
	factory := r.ClientFactory
	if factory == nil {
		factory = sandboxclient.DefaultFactory()
	}
	sandboxClient, err := factory.NewClient(pod.Status.PodIP, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create prebuild sandbox client: %w", err)
	}
	return sandboxClient, nil
}

func (r *RunReconciler) hasPrebuildCompletionMarker(ctx context.Context, run *v1alpha1.Run, prebuildName string) (bool, error) {
	commit := strings.TrimSpace(run.Spec.Repo.Commit)
	if commit == "" {
		return false, nil
	}
	sink := r.artifactSink()
	if sink.ArtifactPrefix(commit) == "" {
		return false, nil
	}
	markerPath := artifacts.PrebuildMarkerTarget(sink, commit, prebuildName)
	if markerPath == "" {
		return false, nil
	}

	loadContext, loadCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.artifactSinkTimeout())
	markerBytes, err := artifacts.LoadWithTimeout(loadContext, sink, markerPath)
	loadCancel()
	if err != nil {
		if artifacts.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if len(markerBytes) == 0 {
		return false, nil
	}

	var marker artifacts.PrebuildCompletionMarker
	if err := json.Unmarshal(markerBytes, &marker); err != nil {
		return false, fmt.Errorf("unmarshal prebuild completion marker %s: %w", markerPath, err)
	}
	if marker.Commit != "" && marker.Commit != commit {
		return false, fmt.Errorf("prebuild completion marker %s commit %q does not match %q", markerPath, marker.Commit, commit)
	}
	if marker.PrebuildName != "" && marker.PrebuildName != prebuildName {
		return false, fmt.Errorf("prebuild completion marker %s prebuildName %q does not match %q", markerPath, marker.PrebuildName, prebuildName)
	}
	return true, nil
}

func (r *RunReconciler) writePrebuildCompletionMarker(ctx context.Context, run *v1alpha1.Run, prebuildName string, completedAt time.Time) error {
	commit := strings.TrimSpace(run.Spec.Repo.Commit)
	if commit == "" {
		return nil
	}
	marker := artifacts.PrebuildCompletionMarker{
		Commit:       commit,
		PrebuildName: prebuildName,
		CompletedAt:  completedAt.UTC(),
	}
	markerBytes, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal prebuild completion marker: %w", err)
	}
	markerBytes = append(markerBytes, '\n')

	storeContext, storeCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.artifactSinkTimeout())
	defer storeCancel()
	_, err = artifacts.StorePrebuildMarkerWithTimeout(storeContext, r.artifactSink(), commit, prebuildName, markerBytes)
	return err
}

func (r *RunReconciler) reconcilePrebuilds(
	ctx context.Context,
	req ctrl.Request,
	run *v1alpha1.Run,
	manifest *v1alpha1.Manifest,
) (prebuildReconcileResult, error) {
	var result prebuildReconcileResult
	if len(run.Status.Prebuilds) == 0 {
		return result, nil
	}

	logger := log.FromContext(ctx).WithValues("run", run.Name)

	var prebuildSandboxList sandboxv1beta1.SandboxList
	if err := r.List(ctx, &prebuildSandboxList, client.InNamespace(r.sandboxNamespace()), client.MatchingLabels{
		discovery.LabelRunName:      run.Name,
		discovery.LabelRunNamespace: run.Namespace,
		discovery.LabelRole:         discovery.RolePrebuild,
	}); err != nil {
		return result, err
	}

	sandboxesByPrebuild := make(map[string]sandboxv1beta1.Sandbox, len(prebuildSandboxList.Items))
	for _, sandbox := range prebuildSandboxList.Items {
		prebuildName := sandbox.Annotations[discovery.AnnotationPrebuildName]
		if prebuildName != "" {
			sandboxesByPrebuild[prebuildName] = sandbox
		}
	}

	// Clean up lingering sandboxes for prebuilds already in a terminal phase.
	for _, prebuildStatus := range run.Status.Prebuilds {
		if prebuildStatus.Phase == v1alpha1.PrebuildPhaseCompleted || prebuildStatus.Phase == v1alpha1.PrebuildPhaseFailed {
			if sandbox, exists := sandboxesByPrebuild[prebuildStatus.Name]; exists {
				r.cleanupPrebuildSandbox(ctx, run.Namespace, run.Name, prebuildCleanupTarget{
					prebuildName: prebuildStatus.Name,
					sandbox:      &sandbox,
				})
				delete(sandboxesByPrebuild, prebuildStatus.Name)
			}
		}
	}

	now := r.now()
	statusNow := metav1.NewTime(now)
	gracePeriod := r.taskGracePeriod()
	maxMessageBytes := r.maxMessageBytes()

	updates := make(map[string]prebuildUpdate)
	var cleanupTargets []prebuildCleanupTarget

	for _, prebuildStatus := range run.Status.Prebuilds {
		switch prebuildStatus.Phase {
		case v1alpha1.PrebuildPhasePending:
			r.reconcilePendingPrebuild(ctx, run, manifest, prebuildStatus, sandboxesByPrebuild, statusNow, maxMessageBytes, updates, &result)
		case v1alpha1.PrebuildPhaseRunning:
			cleanupTarget := r.reconcileRunningPrebuild(ctx, run, manifest, prebuildStatus, sandboxesByPrebuild, now, statusNow, gracePeriod, maxMessageBytes, updates, &result)
			if cleanupTarget != nil {
				cleanupTargets = append(cleanupTargets, *cleanupTarget)
			}
		}
	}

	// Determine whether any Pending tasks need to be failed due to a Failed prebuild.
	effectivePrebuilds := applyPrebuildUpdatesCopy(run.Status.Prebuilds, updates)
	failedDependentTasks := collectFailedDependentTasks(run.Status.Tasks, effectivePrebuilds, manifest, maxMessageBytes)

	if len(updates) > 0 || len(failedDependentTasks) > 0 {
		result.hasStatusUpdates = true
		err := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, generation int64) {
			applyPrebuildStatusUpdates(status, updates, failedDependentTasks, statusNow)
		})
		if err != nil {
			return result, err
		}
		applyPrebuildStatusUpdates(&run.Status, updates, failedDependentTasks, statusNow)
	}

	for _, target := range cleanupTargets {
		r.cleanupPrebuildSandbox(ctx, run.Namespace, run.Name, target)
	}

	if len(failedDependentTasks) > 0 {
		logger.Info("Failed tasks dependent on failed prebuilds", "failedTasks", len(failedDependentTasks))
	}

	return result, nil
}

func (r *RunReconciler) reconcilePendingPrebuild(
	ctx context.Context,
	run *v1alpha1.Run,
	manifest *v1alpha1.Manifest,
	prebuildStatus v1alpha1.PrebuildStatus,
	sandboxesByPrebuild map[string]sandboxv1beta1.Sandbox,
	statusNow metav1.Time,
	maxMessageBytes int,
	updates map[string]prebuildUpdate,
	result *prebuildReconcileResult,
) {
	logger := log.FromContext(ctx).WithValues("run", run.Name, "prebuild", prebuildStatus.Name)

	// 1. Check per-commit cache marker in the artifact store before launching a sandbox.
	cached, cacheErr := r.hasPrebuildCompletionMarker(ctx, run, prebuildStatus.Name)
	if cacheErr != nil {
		logger.Error(cacheErr, "Failed to check prebuild completion marker; proceeding to execute prebuild")
	} else if cached {
		logger.Info("Prebuild completion marker found in artifact store; skipping sandbox execution", "commit", run.Spec.Repo.Commit)
		updates[prebuildStatus.Name] = prebuildUpdate{
			phase:        v1alpha1.PrebuildPhaseCompleted,
			finishedAt:   &statusNow,
			setFinished:  true,
			message:      "cached (completion marker found)",
			executionID:  "",
			setExecution: true,
		}
		return
	}

	// 2. Check for an existing sandbox or terminating pod with the same name.
	sandboxName := discovery.PrebuildSandboxNameForRun(run.Name, prebuildStatus.Name)
	if existingSandbox, exists := sandboxesByPrebuild[prebuildStatus.Name]; exists {
		if !existingSandbox.DeletionTimestamp.IsZero() || existingSandbox.Annotations[discovery.AnnotationControllerInstance] != r.InstanceID {
			r.cleanupPrebuildSandbox(ctx, run.Namespace, run.Name, prebuildCleanupTarget{
				prebuildName: prebuildStatus.Name,
				sandbox:      &existingSandbox,
			})
			result.waitingForOldSandbox = true
			return
		}
	} else {
		var existingPod corev1.Pod
		podKey := types.NamespacedName{Namespace: r.sandboxNamespace(), Name: sandboxName}
		if err := r.Get(ctx, podKey, &existingPod); err == nil {
			result.waitingForOldSandbox = true
			return
		} else if !apierrors.IsNotFound(err) {
			logger.Error(err, "Failed to check existing prebuild pod")
			result.waitingForOldSandbox = true
			return
		}
	}

	// 3. Look up manifest prebuild definition and launch prebuild sandbox.
	manifestPrebuild, exists := manifest.Prebuilds[prebuildStatus.Name]
	if !exists {
		updates[prebuildStatus.Name] = prebuildUpdate{
			phase:       v1alpha1.PrebuildPhaseFailed,
			finishedAt:  &statusNow,
			setFinished: true,
			message:     truncateMessage(fmt.Sprintf("prebuild %q not found in manifest", prebuildStatus.Name), maxMessageBytes),
		}
		return
	}
	if err := validatePrebuildExecutionSpec(prebuildStatus.Name, manifestPrebuild); err != nil {
		updates[prebuildStatus.Name] = prebuildUpdate{
			phase:       v1alpha1.PrebuildPhaseFailed,
			finishedAt:  &statusNow,
			setFinished: true,
			message:     truncateMessage(err.Error(), maxMessageBytes),
		}
		return
	}

	sandboxKey := types.NamespacedName{Namespace: r.sandboxNamespace(), Name: sandboxName}
	authToken, err := r.ensurePrebuildAuthToken(ctx, run, prebuildStatus.Name, sandboxKey)
	if err != nil {
		logger.Error(err, "Failed to ensure prebuild auth token")
		updates[prebuildStatus.Name] = prebuildUpdate{
			phase:       v1alpha1.PrebuildPhaseFailed,
			finishedAt:  &statusNow,
			setFinished: true,
			message:     truncateMessage(fmt.Sprintf("failed to ensure prebuild auth token: %v", err), maxMessageBytes),
		}
		return
	}
	if _, err := r.pki().EnsureServerCertSecret(ctx, sandboxName, r.executionDeadline()+time.Hour); err != nil {
		logger.Error(err, "Failed to ensure prebuild TLS server certificate")
		r.cleanupPrebuildAuthToken(ctx, run.Namespace, run.Name, prebuildStatus.Name, sandboxKey)
		updates[prebuildStatus.Name] = prebuildUpdate{
			phase:       v1alpha1.PrebuildPhaseFailed,
			finishedAt:  &statusNow,
			setFinished: true,
			message:     truncateMessage(fmt.Sprintf("failed to ensure prebuild TLS server cert: %v", err), maxMessageBytes),
		}
		return
	}

	desiredSandbox := r.buildPrebuildSandbox(run, prebuildStatus.Name, manifestPrebuild, sandboxName, authToken)
	if err := r.Create(ctx, desiredSandbox); err != nil && !apierrors.IsAlreadyExists(err) {
		logger.Error(err, "Failed to create prebuild sandbox", "sandbox", desiredSandbox.Name)
		r.cleanupPrebuildAuthToken(ctx, run.Namespace, run.Name, prebuildStatus.Name, sandboxKey)
		updates[prebuildStatus.Name] = prebuildUpdate{
			phase:       v1alpha1.PrebuildPhaseFailed,
			finishedAt:  &statusNow,
			setFinished: true,
			message:     truncateMessage(fmt.Sprintf("failed to create prebuild sandbox: %v", err), maxMessageBytes),
		}
		return
	}

	logger.Info("Created prebuild sandbox", "sandbox", desiredSandbox.Name, "image", manifestPrebuild.Image)
	updates[prebuildStatus.Name] = prebuildUpdate{
		phase:         v1alpha1.PrebuildPhaseRunning,
		startedAt:     &statusNow,
		setStarted:    true,
		clearFinished: true,
		message:       "",
		executionID:   "",
		setExecution:  true,
	}
}

func (r *RunReconciler) reconcileRunningPrebuild(
	ctx context.Context,
	run *v1alpha1.Run,
	manifest *v1alpha1.Manifest,
	prebuildStatus v1alpha1.PrebuildStatus,
	sandboxesByPrebuild map[string]sandboxv1beta1.Sandbox,
	now time.Time,
	statusNow metav1.Time,
	gracePeriod time.Duration,
	maxMessageBytes int,
	updates map[string]prebuildUpdate,
	result *prebuildReconcileResult,
) *prebuildCleanupTarget {
	logger := log.FromContext(ctx).WithValues("run", run.Name, "prebuild", prebuildStatus.Name)

	failPrebuild := func(message string, sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) *prebuildCleanupTarget {
		updates[prebuildStatus.Name] = prebuildUpdate{
			phase:        v1alpha1.PrebuildPhaseFailed,
			finishedAt:   &statusNow,
			setFinished:  true,
			message:      truncateMessage(message, maxMessageBytes),
			executionID:  "",
			setExecution: true,
		}
		if sandbox == nil && pod == nil {
			return nil
		}
		target := &prebuildCleanupTarget{
			prebuildName: prebuildStatus.Name,
			sandbox:      sandbox,
		}
		if pod != nil {
			target.podUID = pod.UID
		}
		return target
	}

	// 1. Observe sandbox.
	sandbox, foundSandbox := sandboxesByPrebuild[prebuildStatus.Name]
	var sandboxPointer *sandboxv1beta1.Sandbox
	if foundSandbox {
		sandboxCopy := sandbox
		sandboxPointer = &sandboxCopy
	} else {
		sandboxName := discovery.PrebuildSandboxNameForRun(run.Name, prebuildStatus.Name)
		sandboxKey := types.NamespacedName{Namespace: r.sandboxNamespace(), Name: sandboxName}
		var fetched sandboxv1beta1.Sandbox
		if err := r.uncachedReader().Get(ctx, sandboxKey, &fetched); err == nil {
			sandboxPointer = &fetched
		} else if !apierrors.IsNotFound(err) {
			logger.Error(err, "Failed to get prebuild sandbox")
			result.waitingForPod = true
			return nil
		}
	}

	if sandboxPointer == nil {
		if prebuildStatus.StartedAt != nil && now.Sub(prebuildStatus.StartedAt.Time) < gracePeriod {
			remaining := gracePeriod - now.Sub(prebuildStatus.StartedAt.Time)
			result.waitingInGracePeriod = true
			if result.minGraceRemaining == 0 || remaining < result.minGraceRemaining {
				result.minGraceRemaining = remaining
			}
			return nil
		}
		return failPrebuild("prebuild sandbox missing while in Running phase", nil, nil)
	}

	// If the sandbox was created by a different controller instance and has not started async execution,
	// reset to Pending so we cleanly recreate it (and re-check cache marker).
	if prebuildStatus.ExecutionID == "" && sandboxPointer.Annotations[discovery.AnnotationControllerInstance] != r.InstanceID {
		updates[prebuildStatus.Name] = prebuildUpdate{
			phase:         v1alpha1.PrebuildPhasePending,
			clearFinished: true,
			message:       "",
			executionID:   "",
			setExecution:  true,
		}
		return &prebuildCleanupTarget{
			prebuildName: prebuildStatus.Name,
			sandbox:      sandboxPointer,
		}
	}

	// 2. Observe backing pod.
	podName := sandboxPointer.Name
	if annotatedPodName := sandboxPointer.Annotations[sandboxv1beta1.SandboxPodNameAnnotation]; annotatedPodName != "" {
		podName = annotatedPodName
	}
	var pod corev1.Pod
	podKey := types.NamespacedName{Namespace: sandboxPointer.Namespace, Name: podName}
	if err := r.Get(ctx, podKey, &pod); err != nil {
		if !apierrors.IsNotFound(err) {
			logger.Error(err, "Failed to get prebuild pod", "pod", podName)
		}
		result.waitingForPod = true
		return nil
	}
	if !isUsableSandboxPod(&pod, sandboxPointer.UID) {
		result.waitingForPod = true
		return nil
	}

	// 3. Bind service portal egress policy before waiting on pod readiness.
	if r.ServicePortalClient != nil && !r.isServicePortalPodBound(pod.UID) {
		if pod.Spec.NodeName == "" {
			result.waitingForPod = true
			return nil
		}
		bindContext, bindCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
		bindErr := r.bindServicePortalPolicy(bindContext, string(pod.UID), r.servicePortalPolicy(), nil, nil)
		bindCancel()
		if bindErr != nil {
			logger.Error(bindErr, "Failed to bind service portal policy for prebuild pod", "podUID", pod.UID)
			return failPrebuild(fmt.Sprintf("failed to bind service portal policy: %v", bindErr), sandboxPointer, &pod)
		}
		r.markServicePortalPodBound(pod.UID)
	}

	// 4. Detect pod or sandbox failure conditions.
	if initFailure := taskPodInitFailureMessage(&pod); initFailure != "" {
		return failPrebuild(initFailure, sandboxPointer, &pod)
	}
	if pod.Status.Phase == corev1.PodFailed {
		return failPrebuild(taskPodFailureMessage(&pod), sandboxPointer, &pod)
	}
	if pod.Status.Phase == corev1.PodSucceeded {
		return failPrebuild("prebuild pod succeeded unexpectedly before command execution", sandboxPointer, &pod)
	}
	finishedCondition := meta.FindStatusCondition(sandboxPointer.Status.Conditions, string(sandboxv1beta1.SandboxConditionFinished))
	if finishedCondition != nil && finishedCondition.Status == metav1.ConditionTrue {
		return failPrebuild(fmt.Sprintf("prebuild sandbox finished unexpectedly: %s", finishedCondition.Message), sandboxPointer, &pod)
	}

	// 5. Wait until pod or sandbox is ready with a PodIP.
	sandboxReady := meta.IsStatusConditionTrue(sandboxPointer.Status.Conditions, string(sandboxv1beta1.SandboxConditionReady))
	if (!sandboxReady && !isPodReady(&pod)) || pod.Status.PodIP == "" {
		result.waitingForPod = true
		return nil
	}

	manifestPrebuild, exists := manifest.Prebuilds[prebuildStatus.Name]
	if !exists {
		return failPrebuild(fmt.Sprintf("prebuild %q not found in manifest", prebuildStatus.Name), sandboxPointer, &pod)
	}

	// 6. Execute / poll async prebuild command.
	prebuildTimeout := r.effectivePrebuildTimeout(manifestPrebuild)
	if prebuildStatus.ExecutionID != "" && prebuildStatus.StartedAt != nil && !now.Before(prebuildStatus.StartedAt.Time.Add(prebuildTimeout)) {
		return failPrebuild(fmt.Sprintf("step prebuild %q failed: sandbox did not report completion within %s", prebuildStatus.Name, formatTaskDuration(prebuildTimeout)), sandboxPointer, &pod)
	}

	clientContext, clientCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
	sandboxClient, err := r.newPrebuildSandboxClient(clientContext, run, prebuildStatus.Name, sandboxPointer, &pod)
	clientCancel()
	if err != nil {
		return failPrebuild(fmt.Sprintf("failed to connect to prebuild sandbox: %v", err), sandboxPointer, &pod)
	}
	defer func() {
		if closeErr := sandboxClient.Close(); closeErr != nil {
			logger.V(1).Info("Failed to close prebuild sandbox client", "error", closeErr)
		}
	}()

	if prebuildStatus.ExecutionID == "" {
		executionID := prebuildExecutionID(run.Name, prebuildStatus.Name)
		workingDirectory := "/workspace"
		if run.Spec.Repo.URL != "" {
			workingDirectory = "repo"
		}
		startContext, startCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
		startErr := sandboxClient.StartAsync(startContext, executionID, sandboxclient.ProcessConfig{
			Command: manifestPrebuild.Command,
			Cwd:     workingDirectory,
		})
		startCancel()
		if startErr != nil && !errors.Is(startErr, sandboxclient.ErrAsyncTaskAlreadyExists) {
			return failPrebuild(fmt.Sprintf("failed to start prebuild %q: %v", prebuildStatus.Name, startErr), sandboxPointer, &pod)
		}
		logger.Info("Started async prebuild command", "executionID", executionID, "command", manifestPrebuild.Command)
		updates[prebuildStatus.Name] = prebuildUpdate{
			phase:        v1alpha1.PrebuildPhaseRunning,
			executionID:  executionID,
			setExecution: true,
		}
		return nil
	}

	pollContext, pollCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
	pollStatus, pollErr := sandboxClient.PollAsync(pollContext, prebuildStatus.ExecutionID)
	pollCancel()
	if pollErr != nil {
		return failPrebuild(fmt.Sprintf("failed to poll prebuild %q: %v", prebuildStatus.Name, pollErr), sandboxPointer, &pod)
	}

	switch pollStatus.State {
	case sandboxclient.AsyncTaskRunning:
		result.pollingAsync = true
		return nil
	case sandboxclient.AsyncTaskNotFound:
		return failPrebuild(fmt.Sprintf("async prebuild %q execution %q not found in sandbox", prebuildStatus.Name, prebuildStatus.ExecutionID), sandboxPointer, &pod)
	case sandboxclient.AsyncTaskCompleted:
		if pollStatus.ExitCode != 0 {
			stderrTail := extractStderrTail(pollStatus.Stderr, r.maxStderrLines(), discovery.MaxStderrTailBytes)
			failureMessage := fmt.Sprintf("prebuild %q exited with code %d", prebuildStatus.Name, pollStatus.ExitCode)
			if stderrTail != "" {
				failureMessage = fmt.Sprintf("%s: %s", failureMessage, stderrTail)
			}
			return failPrebuild(failureMessage, sandboxPointer, &pod)
		}

		// Prebuild exited 0: persist completion marker to artifact store and mark Completed.
		if markerErr := r.writePrebuildCompletionMarker(ctx, run, prebuildStatus.Name, statusNow.Time); markerErr != nil {
			logger.Error(markerErr, "Failed to store prebuild completion marker in artifact store", "commit", run.Spec.Repo.Commit)
		}
		logger.Info("Prebuild completed successfully", "commit", run.Spec.Repo.Commit)
		updates[prebuildStatus.Name] = prebuildUpdate{
			phase:        v1alpha1.PrebuildPhaseCompleted,
			finishedAt:   &statusNow,
			setFinished:  true,
			message:      "",
			executionID:  "",
			setExecution: true,
		}
		return &prebuildCleanupTarget{
			prebuildName: prebuildStatus.Name,
			sandbox:      sandboxPointer,
			podUID:       pod.UID,
		}
	default:
		return failPrebuild(fmt.Sprintf("unexpected async prebuild state %q for %q", pollStatus.State, prebuildStatus.ExecutionID), sandboxPointer, &pod)
	}
}

func (r *RunReconciler) cleanupPrebuildSandbox(ctx context.Context, runNamespace, runName string, target prebuildCleanupTarget) {
	logger := log.FromContext(ctx).WithValues("run", runName, "prebuild", target.prebuildName)
	if target.podUID != "" {
		r.unmarkServicePortalPodBound(target.podUID)
	}
	if target.sandbox == nil {
		return
	}
	sandboxKey := types.NamespacedName{Namespace: target.sandbox.Namespace, Name: target.sandbox.Name}
	if err := r.Delete(ctx, target.sandbox); err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "Failed to delete prebuild sandbox", "sandbox", target.sandbox.Name)
	}
	r.cleanupPrebuildAuthToken(ctx, runNamespace, runName, target.prebuildName, sandboxKey)
}

func applyPrebuildUpdatesCopy(prebuilds []v1alpha1.PrebuildStatus, updates map[string]prebuildUpdate) []v1alpha1.PrebuildStatus {
	copied := make([]v1alpha1.PrebuildStatus, len(prebuilds))
	copy(copied, prebuilds)
	applyPrebuildUpdatesInPlace(copied, updates)
	return copied
}

func applyPrebuildUpdatesInPlace(prebuilds []v1alpha1.PrebuildStatus, updates map[string]prebuildUpdate) {
	for i := range prebuilds {
		update, hasUpdate := updates[prebuilds[i].Name]
		if !hasUpdate {
			continue
		}
		prebuilds[i].Phase = update.phase
		prebuilds[i].Message = update.message
		if update.setStarted {
			prebuilds[i].StartedAt = update.startedAt
		}
		if update.setFinished {
			prebuilds[i].FinishedAt = update.finishedAt
		} else if update.clearFinished {
			prebuilds[i].FinishedAt = nil
		}
		if update.setExecution {
			prebuilds[i].ExecutionID = update.executionID
		}
	}
}

func collectFailedDependentTasks(
	tasks []v1alpha1.TaskStatus,
	prebuilds []v1alpha1.PrebuildStatus,
	manifest *v1alpha1.Manifest,
	maxMessageBytes int,
) map[string]string {
	failedPrebuilds := make(map[string]v1alpha1.PrebuildStatus)
	for _, prebuild := range prebuilds {
		if prebuild.Phase == v1alpha1.PrebuildPhaseFailed {
			failedPrebuilds[prebuild.Name] = prebuild
		}
	}
	if len(failedPrebuilds) == 0 {
		return nil
	}

	prebuildsByTask := taskPrebuildByID(manifest)
	failedTasks := make(map[string]string)
	for _, task := range tasks {
		if task.Phase != v1alpha1.TaskPhasePending {
			continue
		}
		prebuildName := prebuildsByTask[task.ID]
		if prebuildName == "" {
			continue
		}
		failedPrebuild, isFailed := failedPrebuilds[prebuildName]
		if !isFailed {
			continue
		}
		message := fmt.Sprintf("prebuild %q failed", prebuildName)
		if failedPrebuild.Message != "" {
			message = fmt.Sprintf("prebuild %q failed: %s", prebuildName, failedPrebuild.Message)
		}
		failedTasks[task.ID] = truncateMessage(message, maxMessageBytes)
	}
	return failedTasks
}

func applyPrebuildStatusUpdates(
	status *v1alpha1.RunStatus,
	updates map[string]prebuildUpdate,
	failedDependentTasks map[string]string,
	now metav1.Time,
) {
	applyPrebuildUpdatesInPlace(status.Prebuilds, updates)
	if len(failedDependentTasks) > 0 {
		for i := range status.Tasks {
			failureMessage, shouldFail := failedDependentTasks[status.Tasks[i].ID]
			if !shouldFail || status.Tasks[i].Phase != v1alpha1.TaskPhasePending {
				continue
			}
			status.Tasks[i].Phase = v1alpha1.TaskPhaseFailed
			status.Tasks[i].FinishedAt = &now
			status.Tasks[i].Message = failureMessage
		}
		status.Summary = ComputeRunSummary(status.Tasks)
	}
}
