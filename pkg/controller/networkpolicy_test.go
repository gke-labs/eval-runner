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
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/pkg/discovery"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = networkingv1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	return s
}

func TestReconcileSandboxEgress_RenderingsAssertedExactly(t *testing.T) {
	ctx := t.Context()
	s := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(s).Build()
	logger := logr.Discard()
	ns := "eval-runner-sandbox"
	fqdnVersion := discovery.FQDNNetworkPolicyAPIVersion

	// Case 1: FQDN active (CRD present and hosts specified)
	t.Run("FQDNActive", func(t *testing.T) {
		hosts := []string{"github.com", "*.googleapis.com"}
		err := ReconcileSandboxEgress(ctx, cl, ns, true, fqdnVersion, hosts, logger)
		if err != nil {
			t.Fatalf("unexpected error reconciling sandbox egress: %v", err)
		}

		// Verify baseline NetworkPolicy: exactly 2 rules, NO 0.0.0.0/0 internet rule, NO port 443
		baseline := &networkingv1.NetworkPolicy{}
		err = cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: discovery.DefaultSandboxEgressNetworkPolicyName}, baseline)
		if err != nil {
			t.Fatalf("failed to get baseline NetworkPolicy: %v", err)
		}

		if len(baseline.Spec.Egress) != 2 {
			t.Fatalf("expected 2 egress rules when FQDN is active, got %d", len(baseline.Spec.Egress))
		}

		for i, rule := range baseline.Spec.Egress {
			for j, to := range rule.To {
				if to.IPBlock != nil && to.IPBlock.CIDR == "0.0.0.0/0" {
					t.Fatalf("rule %d peer %d: 0.0.0.0/0 must not exist in baseline when FQDN is active", i, j)
				}
			}
			for j, port := range rule.Ports {
				if port.Port != nil && port.Port.IntVal == 443 {
					t.Fatalf("rule %d port %d: port 443 must not exist in baseline when FQDN is active", i, j)
				}
			}
		}

		// Rule 1: DNS
		dnsRule := baseline.Spec.Egress[0]
		if len(dnsRule.To) != 3 || len(dnsRule.Ports) != 2 {
			t.Errorf("expected 3 peers and 2 ports in DNS rule, got %d peers, %d ports", len(dnsRule.To), len(dnsRule.Ports))
		}
		// Rule 2: Git fixture
		gitRule := baseline.Spec.Egress[1]
		if len(gitRule.To) != 1 || gitRule.To[0].PodSelector.MatchLabels["app"] != "git-server" {
			t.Errorf("expected git-server rule, got %v", gitRule.To)
		}

		// Verify FQDNNetworkPolicy created
		fqdn := &unstructured.Unstructured{}
		fqdn.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   discovery.FQDNNetworkPolicyGroup,
			Version: "v1alpha1",
			Kind:    discovery.FQDNNetworkPolicyKind,
		})
		err = cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: discovery.DefaultFQDNNetworkPolicyName}, fqdn)
		if err != nil {
			t.Fatalf("failed to get FQDNNetworkPolicy: %v", err)
		}
		egress := fqdn.Object["spec"].(map[string]any)["egress"].([]any)
		if len(egress) != 1 {
			t.Fatalf("expected 1 egress rule in FQDN policy, got %d", len(egress))
		}
		ports := egress[0].(map[string]any)["ports"].([]any)
		if len(ports) != 1 || ports[0].(map[string]any)["port"].(int64) != 443 {
			t.Errorf("expected FQDN policy port 443, got %v", ports)
		}
	})

	// Case 2: FQDN absent (CRD absent)
	t.Run("FQDNAbsent", func(t *testing.T) {
		clAbsent := fake.NewClientBuilder().WithScheme(s).Build()
		hosts := []string{"github.com"}
		err := ReconcileSandboxEgress(ctx, clAbsent, ns, false, "", hosts, logger)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		baseline := &networkingv1.NetworkPolicy{}
		err = clAbsent.Get(ctx, types.NamespacedName{Namespace: ns, Name: discovery.DefaultSandboxEgressNetworkPolicyName}, baseline)
		if err != nil {
			t.Fatalf("failed to get baseline NetworkPolicy: %v", err)
		}

		if len(baseline.Spec.Egress) != 3 {
			t.Fatalf("expected 3 egress rules when FQDN is absent, got %d", len(baseline.Spec.Egress))
		}

		internetRule := baseline.Spec.Egress[1]
		if internetRule.To[0].IPBlock == nil || internetRule.To[0].IPBlock.CIDR != "0.0.0.0/0" {
			t.Errorf("expected 0.0.0.0/0 in internet rule, got %v", internetRule.To[0].IPBlock)
		}
		expectedPorts := map[int32]bool{80: false, 443: false, 9418: false}
		for _, p := range internetRule.Ports {
			if p.Port != nil {
				expectedPorts[p.Port.IntVal] = true
			}
		}
		for p, found := range expectedPorts {
			if !found {
				t.Errorf("missing port %d in internet rule", p)
			}
		}
	})

	// Case 3: FQDN disabled (hosts is nil/empty)
	t.Run("FQDNDisabled", func(t *testing.T) {
		clDisabled := fake.NewClientBuilder().WithScheme(s).Build()
		err := ReconcileSandboxEgress(ctx, clDisabled, ns, true, fqdnVersion, nil, logger)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		baseline := &networkingv1.NetworkPolicy{}
		err = clDisabled.Get(ctx, types.NamespacedName{Namespace: ns, Name: discovery.DefaultSandboxEgressNetworkPolicyName}, baseline)
		if err != nil {
			t.Fatalf("failed to get baseline NetworkPolicy: %v", err)
		}

		if len(baseline.Spec.Egress) != 3 {
			t.Fatalf("expected 3 egress rules when FQDN is disabled, got %d", len(baseline.Spec.Egress))
		}

		// Verify FQDNNetworkPolicy does not exist
		fqdn := &unstructured.Unstructured{}
		fqdn.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   discovery.FQDNNetworkPolicyGroup,
			Version: "v1alpha1",
			Kind:    discovery.FQDNNetworkPolicyKind,
		})
		err = clDisabled.Get(ctx, types.NamespacedName{Namespace: ns, Name: discovery.DefaultFQDNNetworkPolicyName}, fqdn)
		if err == nil {
			t.Fatal("expected FQDNNetworkPolicy to not exist when hosts is nil")
		}
	})
}

func TestReconcileSandboxEgress_AdoptionByNameUpdatesInPlace(t *testing.T) {
	ctx := t.Context()
	s := newTestScheme()
	logger := logr.Discard()
	ns := "eval-runner-sandbox"
	name := discovery.DefaultSandboxEgressNetworkPolicyName

	// Pre-create an existing NetworkPolicy with a dummy spec
	existing := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       ns,
			ResourceVersion: "10",
			UID:             "uid-12345",
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{"dummy": "old"},
			},
		},
	}
	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(existing).Build()

	err := ReconcileSandboxEgress(ctx, cl, ns, false, "", nil, logger)
	if err != nil {
		t.Fatalf("unexpected error on adoption reconcile: %v", err)
	}

	adopted := &networkingv1.NetworkPolicy{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, adopted)
	if err != nil {
		t.Fatalf("failed to get adopted policy: %v", err)
	}

	if adopted.UID != "uid-12345" {
		t.Errorf("expected UID to be preserved (updated in place), got %s", adopted.UID)
	}
	if len(adopted.Spec.Egress) != 3 {
		t.Errorf("expected 3 egress rules after adoption update, got %d", len(adopted.Spec.Egress))
	}
	if adopted.Spec.PodSelector.MatchLabels[discovery.LabelSandbox] != "true" {
		t.Errorf("expected podSelector %s=true, got %v", discovery.LabelSandbox, adopted.Spec.PodSelector.MatchLabels)
	}

	// Verify never a second egress policy created
	list := &networkingv1.NetworkPolicyList{}
	if err := cl.List(ctx, list, client.InNamespace(ns)); err != nil {
		t.Fatalf("failed to list policies: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected exactly 1 NetworkPolicy in namespace, got %d", len(list.Items))
	}
}

func TestReconcileSandboxEgress_AppendedPortOrPeerRestored_ResourceVersionUnchanged(t *testing.T) {
	ctx := t.Context()
	s := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(s).Build()
	logger := logr.Discard()
	ns := "eval-runner-sandbox"
	name := discovery.DefaultSandboxEgressNetworkPolicyName

	// 1. Initial reconcile
	err := ReconcileSandboxEgress(ctx, cl, ns, false, "", nil, logger)
	if err != nil {
		t.Fatalf("initial reconcile failed: %v", err)
	}

	policy := &networkingv1.NetworkPolicy{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, policy); err != nil {
		t.Fatalf("failed to get policy: %v", err)
	}
	initialRV := policy.ResourceVersion

	// Second reconcile without changes leaves ResourceVersion unchanged
	err = ReconcileSandboxEgress(ctx, cl, ns, false, "", nil, logger)
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, policy); err != nil {
		t.Fatalf("failed to get policy: %v", err)
	}
	if policy.ResourceVersion != initialRV {
		t.Fatalf("expected unchanged ResourceVersion %s, got %s", initialRV, policy.ResourceVersion)
	}

	// 2. Tamper: append an extra port to the internet rule
	tcpProto := corev1.ProtocolTCP
	port22 := intstr.FromInt32(22)
	policy.Spec.Egress[1].Ports = append(policy.Spec.Egress[1].Ports, networkingv1.NetworkPolicyPort{
		Protocol: &tcpProto,
		Port:     &port22,
	})
	if err := cl.Update(ctx, policy); err != nil {
		t.Fatalf("failed to update policy with tampered port: %v", err)
	}

	// 3. Reconcile repairs the appended port
	err = ReconcileSandboxEgress(ctx, cl, ns, false, "", nil, logger)
	if err != nil {
		t.Fatalf("reconcile after tampering failed: %v", err)
	}

	repaired := &networkingv1.NetworkPolicy{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, repaired); err != nil {
		t.Fatalf("failed to get repaired policy: %v", err)
	}
	if len(repaired.Spec.Egress[1].Ports) != 3 {
		t.Fatalf("expected 3 ports after repair, got %d", len(repaired.Spec.Egress[1].Ports))
	}
	repairedRV := repaired.ResourceVersion

	// 4. A subsequent reconcile leaves the ResourceVersion unchanged
	err = ReconcileSandboxEgress(ctx, cl, ns, false, "", nil, logger)
	if err != nil {
		t.Fatalf("subsequent reconcile failed: %v", err)
	}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, repaired); err != nil {
		t.Fatalf("failed to get repaired policy: %v", err)
	}
	if repaired.ResourceVersion != repairedRV {
		t.Fatalf("expected ResourceVersion to remain %s, got %s", repairedRV, repaired.ResourceVersion)
	}
}

func TestReconcileFQDNNetworkPolicy_AppendedPortOrPeerRestored_ResourceVersionUnchanged(t *testing.T) {
	ctx := t.Context()
	s := runtime.NewScheme()
	cl := fake.NewClientBuilder().WithScheme(s).Build()
	logger := logr.Discard()
	ns := "eval-runner-sandbox"
	policyName := discovery.DefaultFQDNNetworkPolicyName
	apiVersion := discovery.FQDNNetworkPolicyAPIVersion

	// 1. Initial reconcile
	hosts := []string{"github.com"}
	err := ReconcileFQDNNetworkPolicy(ctx, cl, apiVersion, ns, hosts, logger)
	if err != nil {
		t.Fatalf("initial reconcile failed: %v", err)
	}

	fqdn := &unstructured.Unstructured{}
	fqdn.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   discovery.FQDNNetworkPolicyGroup,
		Version: "v1alpha1",
		Kind:    discovery.FQDNNetworkPolicyKind,
	})
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: policyName}, fqdn); err != nil {
		t.Fatalf("failed to get FQDN policy: %v", err)
	}
	initialRV := fqdn.GetResourceVersion()

	// Second reconcile without changes leaves ResourceVersion unchanged
	err = ReconcileFQDNNetworkPolicy(ctx, cl, apiVersion, ns, hosts, logger)
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: policyName}, fqdn); err != nil {
		t.Fatalf("failed to get FQDN policy: %v", err)
	}
	if fqdn.GetResourceVersion() != initialRV {
		t.Fatalf("expected unchanged ResourceVersion %s, got %s", initialRV, fqdn.GetResourceVersion())
	}

	// 2. Tamper: append an extra port to the ports slice
	spec := fqdn.Object["spec"].(map[string]any)
	egress := spec["egress"].([]any)
	rule := egress[0].(map[string]any)
	ports := rule["ports"].([]any)
	ports = append(ports, map[string]any{
		"port":     int64(80),
		"protocol": "TCP",
	})
	rule["ports"] = ports
	if err := cl.Update(ctx, fqdn); err != nil {
		t.Fatalf("failed to tamper FQDN policy: %v", err)
	}

	// 3. Reconcile repairs the appended port
	err = ReconcileFQDNNetworkPolicy(ctx, cl, apiVersion, ns, hosts, logger)
	if err != nil {
		t.Fatalf("reconcile after tampering failed: %v", err)
	}

	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: policyName}, fqdn); err != nil {
		t.Fatalf("failed to get repaired FQDN policy: %v", err)
	}
	repairedPorts := fqdn.Object["spec"].(map[string]any)["egress"].([]any)[0].(map[string]any)["ports"].([]any)
	if len(repairedPorts) != 1 {
		t.Fatalf("expected 1 port after repair, got %d", len(repairedPorts))
	}
	repairedRV := fqdn.GetResourceVersion()

	// 4. Second reconcile leaves ResourceVersion unchanged
	err = ReconcileFQDNNetworkPolicy(ctx, cl, apiVersion, ns, hosts, logger)
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: policyName}, fqdn); err != nil {
		t.Fatalf("failed to get FQDN policy: %v", err)
	}
	if fqdn.GetResourceVersion() != repairedRV {
		t.Fatalf("expected ResourceVersion to remain %s, got %s", repairedRV, fqdn.GetResourceVersion())
	}
}

func TestReconcileFQDNNetworkPolicy_ExtraServerDefaultedMapFieldCausesNoUpdate(t *testing.T) {
	ctx := t.Context()
	s := runtime.NewScheme()
	cl := fake.NewClientBuilder().WithScheme(s).Build()
	logger := logr.Discard()
	ns := "eval-runner-sandbox"
	policyName := discovery.DefaultFQDNNetworkPolicyName
	apiVersion := discovery.FQDNNetworkPolicyAPIVersion

	hosts := []string{"github.com"}
	err := ReconcileFQDNNetworkPolicy(ctx, cl, apiVersion, ns, hosts, logger)
	if err != nil {
		t.Fatalf("initial reconcile failed: %v", err)
	}

	fqdn := &unstructured.Unstructured{}
	fqdn.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   discovery.FQDNNetworkPolicyGroup,
		Version: "v1alpha1",
		Kind:    discovery.FQDNNetworkPolicyKind,
	})
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: policyName}, fqdn); err != nil {
		t.Fatalf("failed to get FQDN policy: %v", err)
	}

	// Add server-defaulted map fields to spec
	spec := fqdn.Object["spec"].(map[string]any)
	spec["serverDefaultOption"] = "auto-configured"
	egress := spec["egress"].([]any)
	rule := egress[0].(map[string]any)
	rule["serverRuleOption"] = true

	if err := cl.Update(ctx, fqdn); err != nil {
		t.Fatalf("failed to add server-defaulted fields: %v", err)
	}

	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: policyName}, fqdn); err != nil {
		t.Fatalf("failed to get FQDN policy: %v", err)
	}
	rvBefore := fqdn.GetResourceVersion()

	// Reconcile should recognize subset match on maps and cause NO update
	err = ReconcileFQDNNetworkPolicy(ctx, cl, apiVersion, ns, hosts, logger)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: policyName}, fqdn); err != nil {
		t.Fatalf("failed to get FQDN policy: %v", err)
	}
	if fqdn.GetResourceVersion() != rvBefore {
		t.Fatalf("expected ResourceVersion to remain %s (no update for defaulted map fields), got %s", rvBefore, fqdn.GetResourceVersion())
	}
}

func TestSandboxEgressController_SyncHostsRoundTrip(t *testing.T) {
	ctx := t.Context()
	s := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(s).Build()
	logger := logr.Discard()
	ns := "eval-runner-sandbox"
	fqdnVersion := discovery.FQDNNetworkPolicyAPIVersion

	controller := NewSandboxEgressController(SandboxEgressControllerOptions{
		Client:           cl,
		SandboxNamespace: ns,
		FQDNSupported:    true,
		FQDNVersion:      fqdnVersion,
		InitialHosts:     []string{"github.com"},
		Logger:           logger,
	})

	// 1. Initial reconcile: FQDN active, 2 rules in baseline, FQDN policy created
	if err := controller.Reconcile(ctx); err != nil {
		t.Fatalf("initial reconcile failed: %v", err)
	}
	baseline := &networkingv1.NetworkPolicy{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: discovery.DefaultSandboxEgressNetworkPolicyName}, baseline); err != nil {
		t.Fatalf("failed to get baseline: %v", err)
	}
	if len(baseline.Spec.Egress) != 2 {
		t.Fatalf("expected 2 rules in baseline, got %d", len(baseline.Spec.Egress))
	}

	// 2. SyncHosts with nil (simulating 'sandboxEgressHosts: none')
	if err := controller.SyncHosts(ctx, nil); err != nil {
		t.Fatalf("sync hosts with nil failed: %v", err)
	}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: discovery.DefaultSandboxEgressNetworkPolicyName}, baseline); err != nil {
		t.Fatalf("failed to get baseline: %v", err)
	}
	if len(baseline.Spec.Egress) != 3 {
		t.Fatalf("expected 3 rules in baseline after disabling FQDN, got %d", len(baseline.Spec.Egress))
	}

	fqdn := &unstructured.Unstructured{}
	fqdn.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   discovery.FQDNNetworkPolicyGroup,
		Version: "v1alpha1",
		Kind:    discovery.FQDNNetworkPolicyKind,
	})
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: discovery.DefaultFQDNNetworkPolicyName}, fqdn); err == nil {
		t.Fatalf("expected FQDN policy to be deleted when hosts is nil")
	}

	// 3. Restore back to default hosts
	if err := controller.SyncHosts(ctx, []string{"github.com", "*.googleapis.com"}); err != nil {
		t.Fatalf("sync hosts restoring failed: %v", err)
	}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: discovery.DefaultSandboxEgressNetworkPolicyName}, baseline); err != nil {
		t.Fatalf("failed to get baseline: %v", err)
	}
	if len(baseline.Spec.Egress) != 2 {
		t.Fatalf("expected 2 rules in baseline after restoring FQDN, got %d", len(baseline.Spec.Egress))
	}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: discovery.DefaultFQDNNetworkPolicyName}, fqdn); err != nil {
		t.Fatalf("expected FQDN policy to be recreated: %v", err)
	}
}

func TestSandboxEgressController_DebounceRateLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	s := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(s).Build()
	logger := logr.Discard()
	ns := "eval-runner-sandbox"

	controller := NewSandboxEgressController(SandboxEgressControllerOptions{
		Client:           cl,
		SandboxNamespace: ns,
		FQDNSupported:    false,
		Logger:           logger,
		RateLimit:        20 * time.Millisecond,
		PeriodicInterval: 1 * time.Second,
	})

	// Initial reconcile
	if err := controller.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	go func() {
		_ = controller.Start(ctx)
	}()

	// Fire rapid triggers
	for i := 0; i < 5; i++ {
		controller.trigger()
		time.Sleep(5 * time.Millisecond)
	}

	// Wait for debounce timer to fire and settle
	time.Sleep(60 * time.Millisecond)

	baseline := &networkingv1.NetworkPolicy{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: discovery.DefaultSandboxEgressNetworkPolicyName}, baseline); err != nil {
		t.Fatalf("failed to get baseline: %v", err)
	}
	if len(baseline.Spec.Egress) != 3 {
		t.Fatalf("expected 3 egress rules, got %d", len(baseline.Spec.Egress))
	}
}

func TestReconcileServicePortalEgress(t *testing.T) {
	ctx := t.Context()
	scheme := newTestScheme()
	logger := logr.Discard()
	namespace := "eval-runner-sandbox"
	policyKey := types.NamespacedName{
		Namespace: namespace,
		Name:      discovery.DefaultServicePortalEgressNetworkPolicyName,
	}

	t.Run("CreatesIdempotentAndRepairsDriftWhenEnabled", func(t *testing.T) {
		cl := fake.NewClientBuilder().WithScheme(scheme).Build()

		if err := ReconcileServicePortalEgress(ctx, cl, namespace, true, logger); err != nil {
			t.Fatalf("initial reconcile failed: %v", err)
		}

		policy := &networkingv1.NetworkPolicy{}
		if err := cl.Get(ctx, policyKey, policy); err != nil {
			t.Fatalf("expected service portal egress NetworkPolicy to exist: %v", err)
		}
		if policy.Spec.PodSelector.MatchLabels[discovery.LabelServicePortalIntercept] != "true" {
			t.Fatalf("expected podSelector %s=true, got %v", discovery.LabelServicePortalIntercept, policy.Spec.PodSelector.MatchLabels)
		}
		if len(policy.Spec.Egress) != 1 || len(policy.Spec.Egress[0].Ports) != 4 {
			t.Fatalf("expected 1 egress rule with 4 ports, got %+v", policy.Spec.Egress)
		}
		rvAfterCreate := policy.ResourceVersion

		// Second reconcile is a no-op
		if err := ReconcileServicePortalEgress(ctx, cl, namespace, true, logger); err != nil {
			t.Fatalf("idempotent reconcile failed: %v", err)
		}
		if err := cl.Get(ctx, policyKey, policy); err != nil {
			t.Fatalf("failed to get policy after idempotent reconcile: %v", err)
		}
		if policy.ResourceVersion != rvAfterCreate {
			t.Fatalf("expected ResourceVersion to remain %s, got %s", rvAfterCreate, policy.ResourceVersion)
		}

		// Simulate drift and confirm repair
		policy.Spec.Egress = nil
		if err := cl.Update(ctx, policy); err != nil {
			t.Fatalf("failed to inject drift: %v", err)
		}
		if err := ReconcileServicePortalEgress(ctx, cl, namespace, true, logger); err != nil {
			t.Fatalf("drift repair reconcile failed: %v", err)
		}
		if err := cl.Get(ctx, policyKey, policy); err != nil {
			t.Fatalf("failed to get repaired policy: %v", err)
		}
		if len(policy.Spec.Egress) != 1 || len(policy.Spec.Egress[0].Ports) != 4 {
			t.Fatalf("expected drift to be repaired to 1 rule / 4 ports, got %+v", policy.Spec.Egress)
		}
	})

	t.Run("DeletesStalePolicyWhenDisabled", func(t *testing.T) {
		cl := fake.NewClientBuilder().WithScheme(scheme).Build()

		// No-op when absent
		if err := ReconcileServicePortalEgress(ctx, cl, namespace, false, logger); err != nil {
			t.Fatalf("reconcile when absent failed: %v", err)
		}

		// Create while enabled, then disable via controller reconcile
		enabledController := NewSandboxEgressController(SandboxEgressControllerOptions{
			Client:               cl,
			SandboxNamespace:     namespace,
			ServicePortalEnabled: true,
			Logger:               logger,
		})
		if err := enabledController.Reconcile(ctx); err != nil {
			t.Fatalf("enabled controller reconcile failed: %v", err)
		}
		policy := &networkingv1.NetworkPolicy{}
		if err := cl.Get(ctx, policyKey, policy); err != nil {
			t.Fatalf("expected policy to exist when enabled: %v", err)
		}

		disabledController := NewSandboxEgressController(SandboxEgressControllerOptions{
			Client:               cl,
			SandboxNamespace:     namespace,
			ServicePortalEnabled: false,
			Logger:               logger,
		})
		if err := disabledController.Reconcile(ctx); err != nil {
			t.Fatalf("disabled controller reconcile failed: %v", err)
		}
		if err := cl.Get(ctx, policyKey, policy); err == nil {
			t.Fatal("expected service portal egress NetworkPolicy to be deleted when disabled")
		}
	})
}

func TestReconcileRegistryServicePortalEgress(t *testing.T) {
	ctx := t.Context()
	scheme := newTestScheme()
	logger := logr.Discard()
	namespace := "eval-runner-sandbox"
	policyKey := types.NamespacedName{
		Namespace: namespace,
		Name:      discovery.DefaultRegistryServicePortalEgressNetworkPolicyName,
	}

	t.Run("CreatesIdempotentRepairsDriftAndUpdatesNamespaceWhenEnabled", func(t *testing.T) {
		cl := fake.NewClientBuilder().WithScheme(scheme).Build()

		if err := ReconcileRegistryServicePortalEgress(ctx, cl, namespace, true, DefaultServicePortalsNamespace, logger); err != nil {
			t.Fatalf("initial reconcile failed: %v", err)
		}

		policy := &networkingv1.NetworkPolicy{}
		if err := cl.Get(ctx, policyKey, policy); err != nil {
			t.Fatalf("expected registry service portal egress NetworkPolicy to exist: %v", err)
		}
		if policy.Spec.PodSelector.MatchLabels[discovery.LabelSandbox] != "true" {
			t.Fatalf("expected podSelector %s=true, got %v", discovery.LabelSandbox, policy.Spec.PodSelector.MatchLabels)
		}
		if len(policy.Spec.Egress) != 1 || len(policy.Spec.Egress[0].To) != 1 ||
			policy.Spec.Egress[0].To[0].NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != DefaultServicePortalsNamespace {
			t.Fatalf("unexpected egress rules: %+v", policy.Spec.Egress)
		}
		if len(policy.Spec.Egress[0].Ports) != 1 || policy.Spec.Egress[0].Ports[0].Port.IntVal != discovery.DefaultRegistryServicePortalPort {
			t.Fatalf("expected TCP port %d, got %+v", discovery.DefaultRegistryServicePortalPort, policy.Spec.Egress[0].Ports)
		}
		resourceVersionAfterCreate := policy.ResourceVersion

		// Second reconcile is a no-op.
		if err := ReconcileRegistryServicePortalEgress(ctx, cl, namespace, true, DefaultServicePortalsNamespace, logger); err != nil {
			t.Fatalf("idempotent reconcile failed: %v", err)
		}
		if err := cl.Get(ctx, policyKey, policy); err != nil {
			t.Fatalf("failed to get policy after idempotent reconcile: %v", err)
		}
		if policy.ResourceVersion != resourceVersionAfterCreate {
			t.Fatalf("expected ResourceVersion to remain %s, got %s", resourceVersionAfterCreate, policy.ResourceVersion)
		}

		// Drift is repaired.
		policy.Spec.Egress = nil
		if err := cl.Update(ctx, policy); err != nil {
			t.Fatalf("failed to inject drift: %v", err)
		}
		if err := ReconcileRegistryServicePortalEgress(ctx, cl, namespace, true, "custom-service-portals", logger); err != nil {
			t.Fatalf("drift/namespace update reconcile failed: %v", err)
		}
		if err := cl.Get(ctx, policyKey, policy); err != nil {
			t.Fatalf("failed to get updated policy: %v", err)
		}
		if len(policy.Spec.Egress) != 1 || len(policy.Spec.Egress[0].To) != 1 ||
			policy.Spec.Egress[0].To[0].NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != "custom-service-portals" {
			t.Fatalf("expected updated namespaceSelector custom-service-portals, got %+v", policy.Spec.Egress)
		}
	})

	t.Run("SyncRegistryServicePortalTogglesAndDeletesStalePolicy", func(t *testing.T) {
		cl := fake.NewClientBuilder().WithScheme(scheme).Build()

		controller := NewSandboxEgressController(SandboxEgressControllerOptions{
			Client:                       cl,
			SandboxNamespace:             namespace,
			RegistryServicePortalEnabled: false,
			Logger:                       logger,
		})
		if err := controller.Reconcile(ctx); err != nil {
			t.Fatalf("initial reconcile failed: %v", err)
		}
		policy := &networkingv1.NetworkPolicy{}
		if err := cl.Get(ctx, policyKey, policy); err == nil {
			t.Fatal("expected no registry service portal egress NetworkPolicy when disabled")
		}

		if err := controller.SyncRegistryServicePortal(ctx, true, "custom-service-portals"); err != nil {
			t.Fatalf("SyncRegistryServicePortal enable failed: %v", err)
		}
		if err := cl.Get(ctx, policyKey, policy); err != nil {
			t.Fatalf("expected registry service portal egress NetworkPolicy after enabling: %v", err)
		}
		if policy.Spec.Egress[0].To[0].NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != "custom-service-portals" {
			t.Fatalf("unexpected namespaceSelector: %+v", policy.Spec.Egress[0].To[0].NamespaceSelector)
		}

		if err := controller.SyncRegistryServicePortal(ctx, false, DefaultServicePortalsNamespace); err != nil {
			t.Fatalf("SyncRegistryServicePortal disable failed: %v", err)
		}
		if err := cl.Get(ctx, policyKey, policy); err == nil {
			t.Fatal("expected registry service portal egress NetworkPolicy to be deleted when disabled")
		}
	})
}
