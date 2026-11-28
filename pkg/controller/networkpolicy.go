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
	"fmt"
	"sync"
	"time"

	"github.com/gke-labs/eval-runner/pkg/artifacts/sinkconfig"
	"github.com/gke-labs/eval-runner/pkg/discovery"
	"github.com/go-logr/logr"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// DefaultEgressReconcileRateLimit is the debounce rate-limit applied to watch-triggered reconciles.
	DefaultEgressReconcileRateLimit = 5 * time.Second

	// DefaultEgressReconcileInterval is the periodic reconciliation interval for sandbox egress policies.
	DefaultEgressReconcileInterval = 60 * time.Second
)

// SandboxEgressControllerOptions configures the SandboxEgressController.
type SandboxEgressControllerOptions struct {
	Client                       client.Client
	KubeClient                   kubernetes.Interface
	DynamicClient                dynamic.Interface
	SandboxNamespace             string
	FQDNSupported                bool
	FQDNVersion                  string
	ServicePortalEnabled         bool
	RegistryServicePortalEnabled bool
	ServicePortalsNamespace      string
	InitialHosts                 []string
	Logger                       logr.Logger
	RateLimit                    time.Duration
	PeriodicInterval             time.Duration
}

// SandboxEgressController reconciles and maintains the sandbox baseline egress NetworkPolicy,
// FQDNNetworkPolicy, companion service-portal egress NetworkPolicy, and companion registry
// service portal egress NetworkPolicy. It watches those objects in the sandbox namespace and
// enforces that hostname narrowing removes the 0.0.0.0/0:443 internet allow rule when FQDN
// policies are active.
type SandboxEgressController struct {
	client               client.Client
	kubeClient           kubernetes.Interface
	dynamicClient        dynamic.Interface
	sandboxNamespace     string
	fqdnSupported        bool
	fqdnVersion          string
	servicePortalEnabled bool
	logger               logr.Logger
	rateLimit            time.Duration
	periodicInterval     time.Duration

	// hostsLock protects mutable in-memory policy inputs (currentHosts, registryServicePortalEnabled,
	// servicePortalsNamespace). Never held across API calls.
	hostsLock                    sync.RWMutex
	currentHosts                 []string
	registryServicePortalEnabled bool
	servicePortalsNamespace      string

	triggerCh chan struct{}
}

// NewSandboxEgressController constructs a new SandboxEgressController.
func NewSandboxEgressController(opts SandboxEgressControllerOptions) *SandboxEgressController {
	ns := opts.SandboxNamespace
	if ns == "" {
		ns = discovery.DefaultSandboxNamespace
	}

	rateLimit := opts.RateLimit
	if rateLimit <= 0 {
		rateLimit = DefaultEgressReconcileRateLimit
	}

	periodicInterval := opts.PeriodicInterval
	if periodicInterval <= 0 {
		periodicInterval = DefaultEgressReconcileInterval
	}

	return &SandboxEgressController{
		client:                       opts.Client,
		kubeClient:                   opts.KubeClient,
		dynamicClient:                opts.DynamicClient,
		sandboxNamespace:             ns,
		fqdnSupported:                opts.FQDNSupported,
		fqdnVersion:                  opts.FQDNVersion,
		servicePortalEnabled:         opts.ServicePortalEnabled,
		logger:                       opts.Logger.WithName("sandbox-egress-controller"),
		rateLimit:                    rateLimit,
		periodicInterval:             periodicInterval,
		currentHosts:                 discovery.CloneSandboxEgressHosts(opts.InitialHosts),
		registryServicePortalEnabled: opts.RegistryServicePortalEnabled,
		servicePortalsNamespace:      opts.ServicePortalsNamespace,
		triggerCh:                    make(chan struct{}, 1),
	}
}

// trigger sends a non-blocking trigger to request a reconcile.
func (c *SandboxEgressController) trigger() {
	select {
	case c.triggerCh <- struct{}{}:
	default:
	}
}

// SyncHosts updates the desired egress hosts and immediately reconciles the policies.
// It is called on ConfigMap reload. It does not hold any lock during API calls.
func (c *SandboxEgressController) SyncHosts(ctx context.Context, hosts []string) error {
	c.hostsLock.Lock()
	c.currentHosts = discovery.CloneSandboxEgressHosts(hosts)
	c.hostsLock.Unlock()

	return c.Reconcile(ctx)
}

// SyncRegistryServicePortal updates the desired registry service portal egress settings
// and immediately reconciles the policies. It is called on ConfigMap reload.
func (c *SandboxEgressController) SyncRegistryServicePortal(ctx context.Context, enabled bool, servicePortalsNamespace string) error {
	c.hostsLock.Lock()
	c.registryServicePortalEnabled = enabled
	c.servicePortalsNamespace = servicePortalsNamespace
	c.hostsLock.Unlock()

	return c.Reconcile(ctx)
}

// Reconcile executes a full reconciliation pass of the sandbox egress NetworkPolicy,
// FQDNNetworkPolicy, companion service-portal egress NetworkPolicy, and companion
// registry service portal egress NetworkPolicy.
// No mutexes are held while waiting on the Kubernetes API.
func (c *SandboxEgressController) Reconcile(ctx context.Context) error {
	c.hostsLock.RLock()
	hosts := discovery.CloneSandboxEgressHosts(c.currentHosts)
	registryServicePortalEnabled := c.registryServicePortalEnabled
	servicePortalsNamespace := c.servicePortalsNamespace
	c.hostsLock.RUnlock()

	if err := ReconcileSandboxEgress(ctx, c.client, c.sandboxNamespace, c.fqdnSupported, c.fqdnVersion, hosts, c.logger); err != nil {
		return err
	}
	if err := ReconcileServicePortalEgress(ctx, c.client, c.sandboxNamespace, c.servicePortalEnabled, c.logger); err != nil {
		return err
	}
	return ReconcileRegistryServicePortalEgress(ctx, c.client, c.sandboxNamespace, registryServicePortalEnabled, servicePortalsNamespace, c.logger)
}

// Start begins the background periodic ticker and namespaced watches.
// It implements manager.Runnable.
func (c *SandboxEgressController) Start(ctx context.Context) error {
	// Start namespaced watch for baseline NetworkPolicy
	if c.kubeClient != nil {
		go func() {
			if err := sinkconfig.WatchNamedObject(
				ctx,
				discovery.DefaultSandboxEgressNetworkPolicyName,
				func(ctx context.Context) (metav1.Object, error) {
					return c.kubeClient.NetworkingV1().NetworkPolicies(c.sandboxNamespace).Get(ctx, discovery.DefaultSandboxEgressNetworkPolicyName, metav1.GetOptions{})
				},
				func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
					return c.kubeClient.NetworkingV1().NetworkPolicies(c.sandboxNamespace).Watch(ctx, opts)
				},
				func(obj metav1.Object) {
					c.trigger()
				},
			); err != nil && ctx.Err() == nil {
				c.logger.Error(err, "watch on sandbox egress NetworkPolicy stopped")
			}
		}()

		if c.servicePortalEnabled {
			go func() {
				if err := sinkconfig.WatchNamedObject(
					ctx,
					discovery.DefaultServicePortalEgressNetworkPolicyName,
					func(ctx context.Context) (metav1.Object, error) {
						return c.kubeClient.NetworkingV1().NetworkPolicies(c.sandboxNamespace).Get(ctx, discovery.DefaultServicePortalEgressNetworkPolicyName, metav1.GetOptions{})
					},
					func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
						return c.kubeClient.NetworkingV1().NetworkPolicies(c.sandboxNamespace).Watch(ctx, opts)
					},
					func(obj metav1.Object) {
						c.trigger()
					},
				); err != nil && ctx.Err() == nil {
					c.logger.Error(err, "watch on service portal egress NetworkPolicy stopped")
				}
			}()
		}

		go func() {
			if err := sinkconfig.WatchNamedObject(
				ctx,
				discovery.DefaultRegistryServicePortalEgressNetworkPolicyName,
				func(ctx context.Context) (metav1.Object, error) {
					return c.kubeClient.NetworkingV1().NetworkPolicies(c.sandboxNamespace).Get(ctx, discovery.DefaultRegistryServicePortalEgressNetworkPolicyName, metav1.GetOptions{})
				},
				func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
					return c.kubeClient.NetworkingV1().NetworkPolicies(c.sandboxNamespace).Watch(ctx, opts)
				},
				func(obj metav1.Object) {
					c.trigger()
				},
			); err != nil && ctx.Err() == nil {
				c.logger.Error(err, "watch on registry service portal egress NetworkPolicy stopped")
			}
		}()
	}

	// Start namespaced watch for FQDNNetworkPolicy if supported on cluster
	if c.fqdnSupported && c.dynamicClient != nil && c.fqdnVersion != "" {
		gv, err := schema.ParseGroupVersion(c.fqdnVersion)
		if err != nil {
			c.logger.Error(err, "invalid FQDNNetworkPolicy apiVersion", "version", c.fqdnVersion)
		} else {
			gvr := schema.GroupVersionResource{
				Group:    gv.Group,
				Version:  gv.Version,
				Resource: discovery.FQDNNetworkPolicyResource,
			}
			go func() {
				if err := sinkconfig.WatchNamedObject(
					ctx,
					discovery.DefaultFQDNNetworkPolicyName,
					func(ctx context.Context) (metav1.Object, error) {
						return c.dynamicClient.Resource(gvr).Namespace(c.sandboxNamespace).Get(ctx, discovery.DefaultFQDNNetworkPolicyName, metav1.GetOptions{})
					},
					func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
						return c.dynamicClient.Resource(gvr).Namespace(c.sandboxNamespace).Watch(ctx, opts)
					},
					func(obj metav1.Object) {
						c.trigger()
					},
				); err != nil && ctx.Err() == nil {
					c.logger.Error(err, "watch on FQDNNetworkPolicy stopped")
				}
			}()
		}
	}

	ticker := time.NewTicker(c.periodicInterval)
	defer ticker.Stop()

	var debounceTimer *time.Timer
	var debounceCh <-chan time.Time
	var lastReconcile time.Time

	for {
		select {
		case <-ctx.Done():
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			return nil
		case <-ticker.C:
			if debounceTimer != nil {
				debounceTimer.Stop()
				debounceTimer = nil
				debounceCh = nil
			}
			if err := c.Reconcile(ctx); err != nil {
				c.logger.Error(err, "periodic reconcile of sandbox egress policies failed")
			}
			lastReconcile = time.Now()
		case <-c.triggerCh:
			elapsed := time.Since(lastReconcile)
			if elapsed < c.rateLimit {
				if debounceTimer == nil {
					debounceTimer = time.NewTimer(c.rateLimit - elapsed)
					debounceCh = debounceTimer.C
				}
			} else {
				if err := c.Reconcile(ctx); err != nil {
					c.logger.Error(err, "watch-triggered reconcile of sandbox egress policies failed")
				}
				lastReconcile = time.Now()
			}
		case <-debounceCh:
			debounceTimer = nil
			debounceCh = nil
			if err := c.Reconcile(ctx); err != nil {
				c.logger.Error(err, "rate-limited reconcile of sandbox egress policies failed")
			}
			lastReconcile = time.Now()
		}
	}
}

// ReconcileSandboxEgress reconciles the sandbox baseline egress NetworkPolicy and FQDNNetworkPolicy.
// When FQDN is active (FQDN CRD present in cluster and hosts is not "none"/empty):
// - FQDN policy is reconciled to allow TCP 443 to the host list.
// - Baseline egress policy has its 0.0.0.0/0 internet rule omitted (narrowed).
// When FQDN is absent or disabled:
// - Baseline egress policy has the full 3-rule set including 0.0.0.0/0 on 80/443/9418.
// - Any existing FQDNNetworkPolicy is deleted if the CRD exists.
func ReconcileSandboxEgress(
	ctx context.Context,
	c client.Client,
	namespace string,
	fqdnSupported bool,
	fqdnVersion string,
	hosts []string,
	logger logr.Logger,
) error {
	fqdnActive := fqdnSupported && fqdnVersion != "" && len(hosts) > 0

	if fqdnActive {
		// Active FQDN: Reconcile FQDN policy first so allow-union is established before narrowing baseline.
		if err := ReconcileFQDNNetworkPolicy(ctx, c, fqdnVersion, namespace, hosts, logger); err != nil {
			return fmt.Errorf("failed to reconcile FQDN policy: %w", err)
		}
		if err := reconcileBaselineEgressPolicy(ctx, c, namespace, true, logger); err != nil {
			return fmt.Errorf("failed to reconcile baseline egress policy: %w", err)
		}
	} else {
		// FQDN absent or disabled: Reconcile baseline first to restore full connectivity before removing FQDN policy.
		if err := reconcileBaselineEgressPolicy(ctx, c, namespace, false, logger); err != nil {
			return fmt.Errorf("failed to reconcile baseline egress policy: %w", err)
		}
		if fqdnSupported && fqdnVersion != "" {
			if err := ReconcileFQDNNetworkPolicy(ctx, c, fqdnVersion, namespace, nil, logger); err != nil {
				return fmt.Errorf("failed to clean up FQDN policy: %w", err)
			}
		}
	}

	return nil
}

// reconcileBaselineEgressPolicy ensures the eval-runner-sandbox-egress NetworkPolicy exists
// and matches desired spec. It uses equality.Semantic.DeepEqual to compare desired against existing.
func reconcileBaselineEgressPolicy(
	ctx context.Context,
	c client.Client,
	namespace string,
	fqdnActive bool,
	logger logr.Logger,
) error {
	desired := discovery.RenderSandboxEgressNetworkPolicy(namespace, fqdnActive)
	name := discovery.DefaultSandboxEgressNetworkPolicyName

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		existing := &networkingv1.NetworkPolicy{}
		err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, existing)
		if apierrors.IsNotFound(err) {
			if createErr := c.Create(ctx, desired); createErr != nil {
				if apierrors.IsAlreadyExists(createErr) {
					return apierrors.NewConflict(schema.GroupResource{Group: "networking.k8s.io", Resource: "networkpolicies"}, name, createErr)
				}
				return fmt.Errorf("failed to create sandbox egress NetworkPolicy %s/%s: %w", namespace, name, createErr)
			}
			logger.Info("created sandbox egress NetworkPolicy",
				"namespace", namespace,
				"name", name,
				"fqdnActive", fqdnActive,
			)
			return nil
		} else if err != nil {
			return fmt.Errorf("failed to get sandbox egress NetworkPolicy %s/%s: %w", namespace, name, err)
		}

		if !equality.Semantic.DeepEqual(desired.Spec, existing.Spec) {
			existing.Spec = desired.Spec
			if updateErr := c.Update(ctx, existing); updateErr != nil {
				return updateErr
			}
			logger.Info("updated sandbox egress NetworkPolicy",
				"namespace", namespace,
				"name", name,
				"fqdnActive", fqdnActive,
			)
		}
		return nil
	})
}

// ReconcileServicePortalEgress ensures the companion service-portal egress NetworkPolicy
// exists when service-portal interception is enabled, and removes it when disabled.
func ReconcileServicePortalEgress(
	ctx context.Context,
	c client.Client,
	namespace string,
	enabled bool,
	logger logr.Logger,
) error {
	name := discovery.DefaultServicePortalEgressNetworkPolicyName

	if !enabled {
		existing := &networkingv1.NetworkPolicy{}
		err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, existing)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to get service portal egress NetworkPolicy %s/%s: %w", namespace, name, err)
		}
		if err := c.Delete(ctx, existing); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete service portal egress NetworkPolicy %s/%s: %w", namespace, name, err)
		}
		logger.Info("deleted service portal egress NetworkPolicy",
			"namespace", namespace,
			"name", name,
		)
		return nil
	}

	desired := discovery.RenderServicePortalEgressNetworkPolicy(namespace)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		existing := &networkingv1.NetworkPolicy{}
		err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, existing)
		if apierrors.IsNotFound(err) {
			if createErr := c.Create(ctx, desired); createErr != nil {
				if apierrors.IsAlreadyExists(createErr) {
					return apierrors.NewConflict(schema.GroupResource{Group: "networking.k8s.io", Resource: "networkpolicies"}, name, createErr)
				}
				return fmt.Errorf("failed to create service portal egress NetworkPolicy %s/%s: %w", namespace, name, createErr)
			}
			logger.Info("created service portal egress NetworkPolicy",
				"namespace", namespace,
				"name", name,
			)
			return nil
		} else if err != nil {
			return fmt.Errorf("failed to get service portal egress NetworkPolicy %s/%s: %w", namespace, name, err)
		}

		if !equality.Semantic.DeepEqual(desired.Spec, existing.Spec) {
			existing.Spec = desired.Spec
			if updateErr := c.Update(ctx, existing); updateErr != nil {
				return updateErr
			}
			logger.Info("updated service portal egress NetworkPolicy",
				"namespace", namespace,
				"name", name,
			)
		}
		return nil
	})
}

// ReconcileRegistryServicePortalEgress ensures the companion registry service portal egress
// NetworkPolicy exists when RegistryServicePortalEnabled is true, and removes it when disabled.
func ReconcileRegistryServicePortalEgress(
	ctx context.Context,
	c client.Client,
	namespace string,
	enabled bool,
	servicePortalsNamespace string,
	logger logr.Logger,
) error {
	name := discovery.DefaultRegistryServicePortalEgressNetworkPolicyName

	if !enabled {
		existing := &networkingv1.NetworkPolicy{}
		err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, existing)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to get registry service portal egress NetworkPolicy %s/%s: %w", namespace, name, err)
		}
		if err := c.Delete(ctx, existing); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete registry service portal egress NetworkPolicy %s/%s: %w", namespace, name, err)
		}
		logger.Info("deleted registry service portal egress NetworkPolicy",
			"namespace", namespace,
			"name", name,
		)
		return nil
	}

	desired := discovery.RenderRegistryServicePortalEgressNetworkPolicy(namespace, servicePortalsNamespace)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		existing := &networkingv1.NetworkPolicy{}
		err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, existing)
		if apierrors.IsNotFound(err) {
			if createErr := c.Create(ctx, desired); createErr != nil {
				if apierrors.IsAlreadyExists(createErr) {
					return apierrors.NewConflict(schema.GroupResource{Group: "networking.k8s.io", Resource: "networkpolicies"}, name, createErr)
				}
				return fmt.Errorf("failed to create registry service portal egress NetworkPolicy %s/%s: %w", namespace, name, createErr)
			}
			logger.Info("created registry service portal egress NetworkPolicy",
				"namespace", namespace,
				"name", name,
				"servicePortalsNamespace", servicePortalsNamespace,
			)
			return nil
		} else if err != nil {
			return fmt.Errorf("failed to get registry service portal egress NetworkPolicy %s/%s: %w", namespace, name, err)
		}

		if !equality.Semantic.DeepEqual(desired.Spec, existing.Spec) {
			existing.Spec = desired.Spec
			if updateErr := c.Update(ctx, existing); updateErr != nil {
				return updateErr
			}
			logger.Info("updated registry service portal egress NetworkPolicy",
				"namespace", namespace,
				"name", name,
				"servicePortalsNamespace", servicePortalsNamespace,
			)
		}
		return nil
	})
}
