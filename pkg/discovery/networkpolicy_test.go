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

// Package discovery hosts this NetworkPolicy test because LabelSandbox
// (evalrunner.gke-labs.dev/sandbox) is defined in this package.
package discovery

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd(): %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find repo root (no go.mod found)")
		}
		dir = parent
	}
}

func loadNetworkPolicies(t *testing.T) []networkingv1.NetworkPolicy {
	t.Helper()
	root := findRepoRoot(t)
	manifestPath := filepath.Join(root, "k8s", "sandbox-networkpolicy.yaml")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("failed to read %s: %v", manifestPath, err)
	}

	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	var policies []networkingv1.NetworkPolicy
	for {
		var np networkingv1.NetworkPolicy
		err := decoder.Decode(&np)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("failed to decode NetworkPolicy from %s: %v", manifestPath, err)
		}
		if np.Kind == "NetworkPolicy" {
			policies = append(policies, np)
		}
	}
	return policies
}

func TestSandboxNetworkPolicies(t *testing.T) {
	policies := loadNetworkPolicies(t)
	if len(policies) != 1 {
		t.Fatalf("expected 1 NetworkPolicy in k8s/sandbox-networkpolicy.yaml, got %d", len(policies))
	}

	ingressPolicy := &policies[0]
	if ingressPolicy.Name != "eval-runner-sandbox-ingress" {
		t.Fatalf("expected NetworkPolicy eval-runner-sandbox-ingress, got %s", ingressPolicy.Name)
	}

	if ingressPolicy.Namespace != "eval-runner-sandbox" {
		t.Errorf("expected namespace eval-runner-sandbox, got %q", ingressPolicy.Namespace)
	}
	if ingressPolicy.Spec.PodSelector.MatchLabels[LabelSandbox] != "true" {
		t.Errorf("expected podSelector %s=true, got %v", LabelSandbox, ingressPolicy.Spec.PodSelector.MatchLabels)
	}

	// Assert Ingress policy
	if len(ingressPolicy.Spec.PolicyTypes) != 1 || ingressPolicy.Spec.PolicyTypes[0] != networkingv1.PolicyTypeIngress {
		t.Fatalf("expected policyTypes [Ingress], got %v", ingressPolicy.Spec.PolicyTypes)
	}
	if len(ingressPolicy.Spec.Ingress) != 1 {
		t.Fatalf("expected 1 ingress rule, got %d", len(ingressPolicy.Spec.Ingress))
	}
	rule := ingressPolicy.Spec.Ingress[0]
	if len(rule.From) != 1 {
		t.Fatalf("expected 1 from peer, got %d", len(rule.From))
	}
	from := rule.From[0]
	if from.NamespaceSelector == nil || from.NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != "eval-runner" {
		t.Errorf("expected ingress from namespace eval-runner, got %v", from.NamespaceSelector)
	}
	if from.PodSelector == nil || from.PodSelector.MatchLabels["app"] != "eval-runner-controller" {
		t.Errorf("expected ingress from pod app=eval-runner-controller, got %v", from.PodSelector)
	}
	expectedPorts := map[int32]bool{8080: false, 9090: false}
	for _, port := range rule.Ports {
		if port.Protocol == nil || *port.Protocol != corev1.ProtocolTCP {
			t.Errorf("expected TCP protocol, got %v", port.Protocol)
		}
		if port.Port != nil {
			expectedPorts[port.Port.IntVal] = true
		}
	}
	for portNum, found := range expectedPorts {
		if !found {
			t.Errorf("missing expected ingress port %d", portNum)
		}
	}
}

func TestRenderSandboxEgressNetworkPolicy_Golden(t *testing.T) {
	egressPolicy := RenderSandboxEgressNetworkPolicy(DefaultSandboxNamespace, false)
	if egressPolicy == nil {
		t.Fatal("RenderSandboxEgressNetworkPolicy returned nil")
	}

	if egressPolicy.Name != DefaultSandboxEgressNetworkPolicyName {
		t.Errorf("expected name %s, got %s", DefaultSandboxEgressNetworkPolicyName, egressPolicy.Name)
	}
	if egressPolicy.Namespace != DefaultSandboxNamespace {
		t.Errorf("expected namespace %s, got %s", DefaultSandboxNamespace, egressPolicy.Namespace)
	}
	if egressPolicy.Spec.PodSelector.MatchLabels[LabelSandbox] != "true" {
		t.Errorf("expected podSelector %s=true, got %v", LabelSandbox, egressPolicy.Spec.PodSelector.MatchLabels)
	}

	if len(egressPolicy.Spec.PolicyTypes) != 1 || egressPolicy.Spec.PolicyTypes[0] != networkingv1.PolicyTypeEgress {
		t.Fatalf("expected policyTypes [Egress], got %v", egressPolicy.Spec.PolicyTypes)
	}
	if len(egressPolicy.Spec.Egress) != 3 {
		t.Fatalf("expected 3 egress rules (DNS, Internet HTTP/Git, In-cluster Git fixture), got %d", len(egressPolicy.Spec.Egress))
	}

	// Verify that no egress rule contains a namespaceSelector with NotIn or an empty podSelector: {}
	for i, rule := range egressPolicy.Spec.Egress {
		for j, to := range rule.To {
			if to.NamespaceSelector != nil {
				for _, req := range to.NamespaceSelector.MatchExpressions {
					if req.Operator == metav1.LabelSelectorOpNotIn {
						t.Errorf("rule %d peer %d: namespaceSelector must not use NotIn operator", i, j)
					}
				}
			}
			if to.PodSelector != nil && len(to.PodSelector.MatchLabels) == 0 && len(to.PodSelector.MatchExpressions) == 0 {
				t.Errorf("rule %d peer %d: podSelector must not be empty {}", i, j)
			}
		}
	}

	// Rule 1: DNS
	dnsRule := egressPolicy.Spec.Egress[0]
	hasKubeSystemDNSPods := false
	hasNodeLocalIP := false
	hasCloudDNSIP := false

	for _, to := range dnsRule.To {
		if to.NamespaceSelector != nil && to.NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] == "kube-system" {
			if to.PodSelector != nil {
				for _, expr := range to.PodSelector.MatchExpressions {
					if expr.Key == "k8s-app" && expr.Operator == metav1.LabelSelectorOpIn {
						vals := map[string]bool{}
						for _, v := range expr.Values {
							vals[v] = true
						}
						if vals["kube-dns"] && vals["node-local-dns"] {
							hasKubeSystemDNSPods = true
						}
					}
				}
			}
		}
		if to.IPBlock != nil {
			if to.IPBlock.CIDR == "169.254.20.10/32" {
				hasNodeLocalIP = true
			}
			if to.IPBlock.CIDR == "169.254.169.254/32" {
				hasCloudDNSIP = true
			}
		}
	}

	if !hasKubeSystemDNSPods {
		t.Errorf("expected DNS to kube-system pods with k8s-app In [kube-dns, node-local-dns]")
	}
	if !hasNodeLocalIP {
		t.Errorf("expected DNS to ipBlock 169.254.20.10/32 (NodeLocal DNSCache)")
	}
	if !hasCloudDNSIP {
		t.Errorf("expected DNS to ipBlock 169.254.169.254/32 (Cloud DNS metadata data plane)")
	}

	hasUDP53 := false
	hasTCP53 := false
	for _, port := range dnsRule.Ports {
		if port.Port != nil && port.Port.IntVal == 53 {
			if port.Protocol != nil && *port.Protocol == corev1.ProtocolUDP {
				hasUDP53 = true
			}
			if port.Protocol != nil && *port.Protocol == corev1.ProtocolTCP {
				hasTCP53 = true
			}
		}
	}
	if !hasUDP53 {
		t.Errorf("expected UDP port 53 in DNS egress rule")
	}
	if !hasTCP53 {
		t.Errorf("expected TCP port 53 in DNS egress rule")
	}

	// Rule 2: Internet Git / HTTP / HTTPS
	internetRule := egressPolicy.Spec.Egress[1]
	if len(internetRule.To) != 1 {
		t.Fatalf("expected exactly 1 peer in internet rule, got %d", len(internetRule.To))
	}
	toPeer := internetRule.To[0]
	if toPeer.IPBlock == nil || toPeer.IPBlock.CIDR != "0.0.0.0/0" {
		t.Fatalf("expected ipBlock 0.0.0.0/0 in internet rule, got %v", toPeer.IPBlock)
	}
	if toPeer.NamespaceSelector != nil || toPeer.PodSelector != nil {
		t.Errorf("internet rule peer must only be an ipBlock; got namespaceSelector=%v, podSelector=%v",
			toPeer.NamespaceSelector, toPeer.PodSelector)
	}

	exceptMap := make(map[string]bool)
	for _, exc := range toPeer.IPBlock.Except {
		exceptMap[exc] = true
	}
	expectedExceptions := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"100.64.0.0/10",
		"169.254.0.0/16",
	}
	for _, expectedCIDR := range expectedExceptions {
		if !exceptMap[expectedCIDR] {
			t.Errorf("expected internet rule ipBlock.except to contain %s, got %v", expectedCIDR, toPeer.IPBlock.Except)
		}
	}

	expectedInternetPorts := map[int32]bool{80: false, 443: false, 9418: false}
	for _, port := range internetRule.Ports {
		if port.Protocol == nil || *port.Protocol != corev1.ProtocolTCP {
			t.Errorf("expected TCP protocol, got %v", port.Protocol)
		}
		if port.Port != nil {
			expectedInternetPorts[port.Port.IntVal] = true
		}
	}
	for portNum, found := range expectedInternetPorts {
		if !found {
			t.Errorf("missing expected internet egress port %d", portNum)
		}
	}

	// Rule 3: In-cluster Git Fixture
	gitFixtureRule := egressPolicy.Spec.Egress[2]
	if len(gitFixtureRule.To) != 1 {
		t.Fatalf("expected exactly 1 peer in git fixture rule, got %d", len(gitFixtureRule.To))
	}
	gitPeer := gitFixtureRule.To[0]
	if gitPeer.PodSelector == nil || gitPeer.PodSelector.MatchLabels["app"] != "git-server" {
		t.Errorf("expected podSelector app=git-server in git fixture rule, got %v", gitPeer.PodSelector)
	}
	if gitPeer.NamespaceSelector != nil || gitPeer.IPBlock != nil {
		t.Errorf("git fixture rule must only use podSelector; got namespaceSelector=%v, ipBlock=%v",
			gitPeer.NamespaceSelector, gitPeer.IPBlock)
	}

	if len(gitFixtureRule.Ports) != 1 {
		t.Fatalf("expected 1 port in git fixture rule, got %d", len(gitFixtureRule.Ports))
	}
	gitPort := gitFixtureRule.Ports[0]
	if gitPort.Protocol == nil || *gitPort.Protocol != corev1.ProtocolTCP {
		t.Errorf("expected TCP protocol for git fixture, got %v", gitPort.Protocol)
	}
	if gitPort.Port == nil || gitPort.Port.IntVal != 80 {
		t.Errorf("expected port 80 for git fixture, got %v", gitPort.Port)
	}
}

func TestRenderSandboxEgressNetworkPolicy_FQDNActive(t *testing.T) {
	egressPolicy := RenderSandboxEgressNetworkPolicy(DefaultSandboxNamespace, true)
	if egressPolicy == nil {
		t.Fatal("RenderSandboxEgressNetworkPolicy returned nil")
	}

	if len(egressPolicy.Spec.Egress) != 2 {
		t.Fatalf("expected exactly 2 egress rules when FQDN is active (DNS and git-server), got %d", len(egressPolicy.Spec.Egress))
	}

	// Assert no 0.0.0.0/0 internet rule and no port 443 rule
	for i, rule := range egressPolicy.Spec.Egress {
		for j, to := range rule.To {
			if to.IPBlock != nil && to.IPBlock.CIDR == "0.0.0.0/0" {
				t.Fatalf("rule %d peer %d: 0.0.0.0/0 must be omitted when FQDN is active", i, j)
			}
		}
		for j, port := range rule.Ports {
			if port.Port != nil && port.Port.IntVal == 443 {
				t.Fatalf("rule %d port %d: port 443 must be omitted from baseline policy when FQDN is active", i, j)
			}
		}
	}

	// Rule 1: DNS
	dnsRule := egressPolicy.Spec.Egress[0]
	if len(dnsRule.To) != 3 {
		t.Errorf("expected 3 peers in DNS rule, got %d", len(dnsRule.To))
	}
	// Rule 2: Git fixture
	gitRule := egressPolicy.Spec.Egress[1]
	if len(gitRule.To) != 1 || gitRule.To[0].PodSelector == nil || gitRule.To[0].PodSelector.MatchLabels["app"] != "git-server" {
		t.Errorf("expected git-server rule, got %v", gitRule.To)
	}
}

func TestRenderFQDNNetworkPolicy_DefaultHosts(t *testing.T) {
	u := RenderFQDNNetworkPolicy(DefaultFQDNNetworkPolicyName, DefaultSandboxNamespace, DefaultSandboxEgressHosts)
	if u == nil {
		t.Fatal("expected non-nil unstructured object")
	}

	if u.GetAPIVersion() != FQDNNetworkPolicyAPIVersion {
		t.Errorf("expected apiVersion %s, got %s", FQDNNetworkPolicyAPIVersion, u.GetAPIVersion())
	}
	if u.GetKind() != FQDNNetworkPolicyKind {
		t.Errorf("expected kind %s, got %s", FQDNNetworkPolicyKind, u.GetKind())
	}
	if u.GetName() != DefaultFQDNNetworkPolicyName {
		t.Errorf("expected name %s, got %s", DefaultFQDNNetworkPolicyName, u.GetName())
	}
	if u.GetNamespace() != DefaultSandboxNamespace {
		t.Errorf("expected namespace %s, got %s", DefaultSandboxNamespace, u.GetNamespace())
	}

	spec, ok := u.Object["spec"].(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any spec, got %T", u.Object["spec"])
	}

	// Verify podSelector
	podSelector, ok := spec["podSelector"].(map[string]any)
	if !ok {
		t.Fatalf("expected podSelector in spec")
	}
	matchLabels, ok := podSelector["matchLabels"].(map[string]any)
	if !ok {
		t.Fatalf("expected matchLabels in podSelector")
	}
	if matchLabels[LabelSandbox] != "true" {
		t.Errorf("expected podSelector %s=true, got %v", LabelSandbox, matchLabels[LabelSandbox])
	}

	// Verify egress rule
	egress, ok := spec["egress"].([]any)
	if !ok || len(egress) != 1 {
		t.Fatalf("expected 1 egress rule, got %v", spec["egress"])
	}
	rule, ok := egress[0].(map[string]any)
	if !ok {
		t.Fatalf("expected map rule, got %T", egress[0])
	}

	// Verify ports: TCP 443
	ports, ok := rule["ports"].([]any)
	if !ok || len(ports) != 1 {
		t.Fatalf("expected 1 port, got %v", rule["ports"])
	}
	port, ok := ports[0].(map[string]any)
	if !ok {
		t.Fatalf("expected map port, got %T", ports[0])
	}
	if port["protocol"] != "TCP" || port["port"] != int64(443) {
		t.Errorf("expected TCP 443, got %v", port)
	}

	// Verify matches
	matches, ok := rule["matches"].([]any)
	if !ok || len(matches) != len(DefaultSandboxEgressHosts) {
		t.Fatalf("expected %d matches, got %d (%v)", len(DefaultSandboxEgressHosts), len(matches), rule["matches"])
	}

	// Check each host is rendered with pattern (for wildcards) or name (for exact)
	for i, host := range DefaultSandboxEgressHosts {
		m, ok := matches[i].(map[string]any)
		if !ok {
			t.Fatalf("expected match map, got %T", matches[i])
		}
		if strings.Contains(host, "*") {
			if m["pattern"] != host {
				t.Errorf("host %s: expected pattern %q, got %v", host, host, m["pattern"])
			}
			if _, hasName := m["name"]; hasName {
				t.Errorf("host %s: pattern match should not have name field", host)
			}
		} else {
			if m["name"] != host {
				t.Errorf("host %s: expected name %q, got %v", host, host, m["name"])
			}
			if _, hasPattern := m["pattern"]; hasPattern {
				t.Errorf("host %s: name match should not have pattern field", host)
			}
		}
	}
}

func TestRenderFQDNNetworkPolicy_CustomHosts(t *testing.T) {
	customHosts := []string{
		"*.googleapis.com",
		"github.com",
		"api.openai.com",
		"*.anthropic.com",
		"  github.com  ", // duplicate with whitespace
	}

	u := RenderFQDNNetworkPolicy("custom-fqdn-policy", "custom-ns", customHosts)
	if u.GetName() != "custom-fqdn-policy" || u.GetNamespace() != "custom-ns" {
		t.Errorf("unexpected name/namespace: %s/%s", u.GetNamespace(), u.GetName())
	}

	spec := u.Object["spec"].(map[string]any)
	egress := spec["egress"].([]any)
	rule := egress[0].(map[string]any)
	matches := rule["matches"].([]any)

	// Should be deduplicated to 4 items
	if len(matches) != 4 {
		t.Fatalf("expected 4 matches after deduplication, got %d", len(matches))
	}

	expectedMatches := []map[string]string{
		{"pattern": "*.googleapis.com"},
		{"name": "github.com"},
		{"name": "api.openai.com"},
		{"pattern": "*.anthropic.com"},
	}

	for i, exp := range expectedMatches {
		m := matches[i].(map[string]any)
		for k, v := range exp {
			if m[k] != v {
				t.Errorf("match %d: expected %s=%q, got %v", i, k, v, m[k])
			}
		}
	}
}

func TestRenderFQDNNetworkPolicy_VersionFallback(t *testing.T) {
	hosts := []string{"github.com", "*.googleapis.com"}
	u := RenderFQDNNetworkPolicyWithVersion(FQDNNetworkPolicyAPIVersionV1Alpha3, "legacy-policy", "legacy-ns", hosts)

	if u.GetAPIVersion() != FQDNNetworkPolicyAPIVersionV1Alpha3 {
		t.Errorf("expected apiVersion %s, got %s", FQDNNetworkPolicyAPIVersionV1Alpha3, u.GetAPIVersion())
	}

	spec := u.Object["spec"].(map[string]any)
	egress := spec["egress"].([]any)
	rule := egress[0].(map[string]any)

	// v1alpha3 uses to[].fqdns
	toPeers, ok := rule["to"].([]any)
	if !ok || len(toPeers) != 1 {
		t.Fatalf("expected to peers in v1alpha3 rule, got %v", rule["to"])
	}
	toPeer := toPeers[0].(map[string]any)
	fqdns, ok := toPeer["fqdns"].([]any)
	if !ok || len(fqdns) != 2 {
		t.Fatalf("expected 2 fqdns in v1alpha3, got %v", toPeer["fqdns"])
	}
	if fqdns[0] != "github.com" || fqdns[1] != "*.googleapis.com" {
		t.Errorf("unexpected fqdns: %v", fqdns)
	}
}

func TestRenderFQDNNetworkPolicy_EmptyHosts(t *testing.T) {
	u := RenderFQDNNetworkPolicy("empty-policy", "empty-ns", nil)
	spec := u.Object["spec"].(map[string]any)
	if _, hasEgress := spec["egress"]; hasEgress {
		t.Errorf("expected no egress rule for empty hosts, got %v", spec["egress"])
	}
}

func TestRenderFQDNNetworkPolicyYAML_RoundTrip(t *testing.T) {
	yamlStr, err := RenderFQDNNetworkPolicyYAML(FQDNNetworkPolicyAPIVersion, DefaultFQDNNetworkPolicyName, DefaultSandboxNamespace, DefaultSandboxEgressHosts)
	if err != nil {
		t.Fatalf("failed to render YAML: %v", err)
	}

	if !strings.Contains(yamlStr, "apiVersion: networking.gke.io/v1alpha1") {
		t.Errorf("expected apiVersion in YAML, got:\n%s", yamlStr)
	}
	if !strings.Contains(yamlStr, "kind: FQDNNetworkPolicy") {
		t.Errorf("expected kind in YAML, got:\n%s", yamlStr)
	}
	if !strings.Contains(yamlStr, "eval-runner-sandbox-fqdn-egress") {
		t.Errorf("expected policy name in YAML, got:\n%s", yamlStr)
	}
	if !strings.Contains(yamlStr, "port: 443") {
		t.Errorf("expected port 443 in YAML, got:\n%s", yamlStr)
	}
	if !strings.Contains(yamlStr, "pattern: '*.googleapis.com'") && !strings.Contains(yamlStr, "pattern: \"*.googleapis.com\"") && !strings.Contains(yamlStr, "pattern: \\*.googleapis.com") {
		t.Errorf("expected pattern for googleapis in YAML, got:\n%s", yamlStr)
	}
}

func TestParseHostList(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"", nil},
		{"   ", nil},
		{"github.com", []string{"github.com"}},
		{"github.com, *.googleapis.com", []string{"github.com", "*.googleapis.com"}},
		{" github.com , *.googleapis.com , github.com ", []string{"github.com", "*.googleapis.com"}},
		{"a.com,,b.com,  ,", []string{"a.com", "b.com"}},
	}

	for _, tt := range tests {
		got := ParseHostList(tt.input)
		if len(got) != len(tt.expected) {
			t.Errorf("ParseHostList(%q) = %v, expected %v", tt.input, got, tt.expected)
			continue
		}
		for i := range got {
			if got[i] != tt.expected[i] {
				t.Errorf("ParseHostList(%q)[%d] = %q, expected %q", tt.input, i, got[i], tt.expected[i])
			}
		}
	}
}

func TestRenderServicePortalEgressNetworkPolicy_Golden(t *testing.T) {
	policy := RenderServicePortalEgressNetworkPolicy(DefaultSandboxNamespace)
	if policy == nil {
		t.Fatal("RenderServicePortalEgressNetworkPolicy returned nil")
	}

	if policy.Name != DefaultServicePortalEgressNetworkPolicyName {
		t.Errorf("expected name %s, got %s", DefaultServicePortalEgressNetworkPolicyName, policy.Name)
	}
	if policy.Namespace != DefaultSandboxNamespace {
		t.Errorf("expected namespace %s, got %s", DefaultSandboxNamespace, policy.Namespace)
	}
	if len(policy.Spec.PodSelector.MatchLabels) != 1 || policy.Spec.PodSelector.MatchLabels[LabelServicePortalIntercept] != "true" {
		t.Errorf("expected podSelector %s=true, got %v", LabelServicePortalIntercept, policy.Spec.PodSelector.MatchLabels)
	}
	if len(policy.Spec.PolicyTypes) != 1 || policy.Spec.PolicyTypes[0] != networkingv1.PolicyTypeEgress {
		t.Fatalf("expected policyTypes [Egress], got %v", policy.Spec.PolicyTypes)
	}
	if len(policy.Spec.Egress) != 1 {
		t.Fatalf("expected 1 egress rule, got %d", len(policy.Spec.Egress))
	}

	rule := policy.Spec.Egress[0]
	if len(rule.To) != 0 {
		t.Errorf("expected empty To peers (all destinations on local proxy ports), got %v", rule.To)
	}

	type protoPort struct {
		proto corev1.Protocol
		port  int32
	}
	expectedPorts := map[protoPort]bool{
		{proto: corev1.ProtocolUDP, port: 1053}: false,
		{proto: corev1.ProtocolTCP, port: 1053}: false,
		{proto: corev1.ProtocolTCP, port: 8080}: false,
		{proto: corev1.ProtocolTCP, port: 8443}: false,
	}
	if len(rule.Ports) != len(expectedPorts) {
		t.Fatalf("expected %d ports, got %d", len(expectedPorts), len(rule.Ports))
	}
	for _, port := range rule.Ports {
		if port.Protocol == nil || port.Port == nil {
			t.Fatalf("expected explicit protocol and port, got %+v", port)
		}
		key := protoPort{proto: *port.Protocol, port: port.Port.IntVal}
		if _, ok := expectedPorts[key]; !ok {
			t.Errorf("unexpected port %s/%d", key.proto, key.port)
		}
		expectedPorts[key] = true
	}
	for key, found := range expectedPorts {
		if !found {
			t.Errorf("missing expected port %s/%d", key.proto, key.port)
		}
	}
}

func TestRenderRegistryServicePortalEgressNetworkPolicy_Golden(t *testing.T) {
	testCases := []struct {
		name                        string
		sandboxNamespace            string
		servicePortalsNamespace     string
		wantSandboxNamespace        string
		wantServicePortalsNamespace string
	}{
		{
			name:                        "defaults when empty",
			sandboxNamespace:            "",
			servicePortalsNamespace:     "",
			wantSandboxNamespace:        DefaultSandboxNamespace,
			wantServicePortalsNamespace: DefaultServicePortalsNamespace,
		},
		{
			name:                        "custom service portals namespace",
			sandboxNamespace:            "custom-sandbox",
			servicePortalsNamespace:     "custom-service-portals",
			wantSandboxNamespace:        "custom-sandbox",
			wantServicePortalsNamespace: "custom-service-portals",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			policy := RenderRegistryServicePortalEgressNetworkPolicy(testCase.sandboxNamespace, testCase.servicePortalsNamespace)
			if policy == nil {
				t.Fatal("RenderRegistryServicePortalEgressNetworkPolicy returned nil")
			}
			if policy.Name != DefaultRegistryServicePortalEgressNetworkPolicyName {
				t.Errorf("expected name %s, got %s", DefaultRegistryServicePortalEgressNetworkPolicyName, policy.Name)
			}
			if policy.Namespace != testCase.wantSandboxNamespace {
				t.Errorf("expected namespace %s, got %s", testCase.wantSandboxNamespace, policy.Namespace)
			}
			if len(policy.Spec.PodSelector.MatchLabels) != 1 || policy.Spec.PodSelector.MatchLabels[LabelSandbox] != "true" {
				t.Errorf("expected podSelector %s=true, got %v", LabelSandbox, policy.Spec.PodSelector.MatchLabels)
			}
			if len(policy.Spec.PolicyTypes) != 1 || policy.Spec.PolicyTypes[0] != networkingv1.PolicyTypeEgress {
				t.Fatalf("expected policyTypes [Egress], got %v", policy.Spec.PolicyTypes)
			}
			if len(policy.Spec.Egress) != 1 {
				t.Fatalf("expected 1 egress rule, got %d", len(policy.Spec.Egress))
			}

			rule := policy.Spec.Egress[0]
			if len(rule.To) != 1 {
				t.Fatalf("expected 1 peer, got %d", len(rule.To))
			}
			peer := rule.To[0]
			if peer.NamespaceSelector == nil || peer.NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != testCase.wantServicePortalsNamespace {
				t.Errorf("expected namespaceSelector kubernetes.io/metadata.name=%q, got %+v", testCase.wantServicePortalsNamespace, peer.NamespaceSelector)
			}
			if peer.PodSelector != nil {
				t.Errorf("expected nil podSelector so any registry pod labels in namespace match, got %+v", peer.PodSelector)
			}
			if len(rule.Ports) != 1 {
				t.Fatalf("expected 1 port, got %d", len(rule.Ports))
			}
			port := rule.Ports[0]
			if port.Protocol == nil || *port.Protocol != corev1.ProtocolTCP {
				t.Errorf("expected TCP protocol, got %v", port.Protocol)
			}
			if port.Port == nil || port.Port.IntVal != DefaultRegistryServicePortalPort {
				t.Errorf("expected port %d, got %v", DefaultRegistryServicePortalPort, port.Port)
			}
		})
	}
}
