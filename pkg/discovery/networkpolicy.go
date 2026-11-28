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

package discovery

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	discoveryk8s "k8s.io/client-go/discovery"
	"sigs.k8s.io/yaml"
)

const (
	// DefaultSandboxEgressNetworkPolicyName is the default name for the sandbox egress NetworkPolicy.
	DefaultSandboxEgressNetworkPolicyName = "eval-runner-sandbox-egress"

	// DefaultServicePortalEgressNetworkPolicyName is the default name for the companion
	// NetworkPolicy permitting intercepted sandbox pods to reach the local service-portal node proxy.
	DefaultServicePortalEgressNetworkPolicyName = "eval-runner-sandbox-service-portal-egress"

	// DefaultRegistryServicePortalEgressNetworkPolicyName is the default name for the companion
	// NetworkPolicy permitting sandbox pods to reach the in-cluster OCI registry service portal.
	DefaultRegistryServicePortalEgressNetworkPolicyName = "eval-runner-sandbox-registry-service-portal-egress"

	// DefaultServicePortalsNamespace is the default Kubernetes namespace where
	// ecosystem caching service portals run.
	DefaultServicePortalsNamespace = "eval-runner-service-portals"

	// DefaultRegistryServicePortalPort is the TCP port for the in-cluster OCI registry service portal.
	DefaultRegistryServicePortalPort int32 = 5000

	// DefaultFQDNNetworkPolicyName is the default name for the sandbox FQDNNetworkPolicy.
	DefaultFQDNNetworkPolicyName = "eval-runner-sandbox-fqdn-egress"

	// FQDNNetworkPolicyAPIVersion is the built-in GKE Dataplane V2 API version for FQDNNetworkPolicy (using matches[].name|pattern).
	FQDNNetworkPolicyAPIVersion = "networking.gke.io/v1alpha1"

	// FQDNNetworkPolicyAPIVersionV1Alpha3 is the API version used by the open-source gke-fqdnnetworkpolicies-golang operator
	// (using to[].fqdns), rather than the built-in GKE Dataplane V2 feature.
	FQDNNetworkPolicyAPIVersionV1Alpha3 = "networking.gke.io/v1alpha3"

	// FQDNNetworkPolicyKind is the resource kind for FQDNNetworkPolicy.
	FQDNNetworkPolicyKind = "FQDNNetworkPolicy"

	// FQDNNetworkPolicyGroup is the API group for GKE networking resources.
	FQDNNetworkPolicyGroup = "networking.gke.io"

	// FQDNNetworkPolicyResource is the plural resource name for FQDNNetworkPolicy.
	FQDNNetworkPolicyResource = "fqdnnetworkpolicies"
)

// DefaultSandboxEgressHosts defines the default allow-list of FQDN hostnames
// permitted for HTTPS egress from sandbox pods on GKE Dataplane V2:
// - Model APIs (*.googleapis.com)
// - Git hosts (github.com, *.github.com, *.githubusercontent.com)
// - Python package repositories (pypi.org, *.pypi.org, files.pythonhosted.org, *.pythonhosted.org)
var DefaultSandboxEgressHosts = []string{
	"*.googleapis.com",
	"github.com",
	"*.github.com",
	"*.githubusercontent.com",
	"pypi.org",
	"*.pypi.org",
	"files.pythonhosted.org",
	"*.pythonhosted.org",
}

// CloneSandboxEgressHosts returns a defensive copy of the provided hosts slice.
func CloneSandboxEgressHosts(hosts []string) []string {
	if hosts == nil {
		return nil
	}
	out := make([]string, len(hosts))
	copy(out, hosts)
	return out
}

// ParseHostList parses a comma-separated list of hostnames, trimming whitespace,
// dropping empty items, and deduplicating while preserving order.
func ParseHostList(s string) []string {
	var hosts []string
	seen := make(map[string]bool)
	for _, h := range strings.Split(s, ",") {
		trimmed := strings.TrimSpace(h)
		if trimmed != "" && !seen[trimmed] {
			seen[trimmed] = true
			hosts = append(hosts, trimmed)
		}
	}
	return hosts
}

// RenderFQDNNetworkPolicy renders an FQDNNetworkPolicy using the default GKE Dataplane V2 API version (v1alpha1).
func RenderFQDNNetworkPolicy(name, namespace string, hosts []string) *unstructured.Unstructured {
	return RenderFQDNNetworkPolicyWithVersion(FQDNNetworkPolicyAPIVersion, name, namespace, hosts)
}

// RenderFQDNNetworkPolicyWithVersion renders an FQDNNetworkPolicy unstructured object for the given API version,
// targeting sandbox pods (LabelSandbox: "true") in the specified namespace and allowing TCP 443 egress to the given hosts.
func RenderFQDNNetworkPolicyWithVersion(apiVersion, name, namespace string, hosts []string) *unstructured.Unstructured {
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		gv = schema.GroupVersion{Group: FQDNNetworkPolicyGroup, Version: "v1alpha1"}
	}

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   gv.Group,
		Version: gv.Version,
		Kind:    FQDNNetworkPolicyKind,
	})
	u.SetName(name)
	u.SetNamespace(namespace)

	// Clean and deduplicate hosts
	var cleanHosts []string
	seen := make(map[string]bool)
	for _, h := range hosts {
		trimmed := strings.TrimSpace(h)
		if trimmed != "" && !seen[trimmed] {
			seen[trimmed] = true
			cleanHosts = append(cleanHosts, trimmed)
		}
	}

	podSelector := map[string]any{
		"matchLabels": map[string]any{
			LabelSandbox: "true",
		},
	}

	ports := []any{
		map[string]any{
			"protocol": "TCP",
			"port":     int64(443),
		},
	}

	var egress []any
	if len(cleanHosts) > 0 {
		if gv.Version == "v1alpha3" {
			// v1alpha3 spec uses egress[].to[].fqdns
			toFQDNs := make([]any, len(cleanHosts))
			for i, h := range cleanHosts {
				toFQDNs[i] = h
			}
			egress = []any{
				map[string]any{
					"to": []any{
						map[string]any{
							"fqdns": toFQDNs,
						},
					},
					"ports": ports,
				},
			}
		} else {
			// v1alpha1 (built-in GKE Dataplane V2) spec uses egress[].matches[].name / pattern
			matches := make([]any, len(cleanHosts))
			for i, h := range cleanHosts {
				if strings.Contains(h, "*") {
					matches[i] = map[string]any{
						"pattern": h,
					}
				} else {
					matches[i] = map[string]any{
						"name": h,
					}
				}
			}
			egress = []any{
				map[string]any{
					"matches": matches,
					"ports":   ports,
				},
			}
		}
	}

	spec := map[string]any{
		"podSelector": podSelector,
	}
	if len(egress) > 0 {
		spec["egress"] = egress
	}

	u.Object["spec"] = spec
	return u
}

// RenderFQDNNetworkPolicyYAML renders the policy and returns its YAML string representation.
func RenderFQDNNetworkPolicyYAML(apiVersion, name, namespace string, hosts []string) (string, error) {
	u := RenderFQDNNetworkPolicyWithVersion(apiVersion, name, namespace, hosts)
	data, err := yaml.Marshal(u.Object)
	if err != nil {
		return "", fmt.Errorf("failed to marshal FQDNNetworkPolicy to YAML: %w", err)
	}
	return string(data), nil
}

// DetectFQDNNetworkPolicyAPIVersion queries discoveryClient to check if FQDNNetworkPolicy CRD
// is registered in the cluster. It checks networking.gke.io/v1alpha1 first, then networking.gke.io/v1alpha3.
// Returns the detected apiVersion and true if found, or ("", false, nil) if not found.
func DetectFQDNNetworkPolicyAPIVersion(discoveryClient discoveryk8s.DiscoveryInterface) (string, bool, error) {
	if discoveryClient == nil {
		return "", false, nil
	}

	candidateVersions := []string{
		FQDNNetworkPolicyAPIVersion,
		FQDNNetworkPolicyAPIVersionV1Alpha3,
	}

	for _, gv := range candidateVersions {
		resources, err := discoveryClient.ServerResourcesForGroupVersion(gv)
		if err != nil {
			if apierrors.IsNotFound(err) || discoveryk8s.IsGroupDiscoveryFailedError(err) {
				// Group or version not found is expected on clusters without FQDN support
				continue
			}
			return "", false, fmt.Errorf("failed to query discovery for %s: %w", gv, err)
		}
		for _, r := range resources.APIResources {
			if r.Kind == FQDNNetworkPolicyKind {
				return gv, true, nil
			}
		}
	}

	return "", false, nil
}

// RenderSandboxEgressNetworkPolicy renders the baseline sandbox egress NetworkPolicy.
// When fqdnActive is true, the 0.0.0.0/0 internet rule (allowing TCP 80/443/9418) is omitted
// so that HTTPS traffic is narrowed to the active FQDNNetworkPolicy.
// When fqdnActive is false (FQDN absent or disabled), the internet rule is included to allow
// external clones and model APIs over 80/443/9418.
func RenderSandboxEgressNetworkPolicy(namespace string, fqdnActive bool) *networkingv1.NetworkPolicy {
	return RenderSandboxEgressNetworkPolicyWithName(DefaultSandboxEgressNetworkPolicyName, namespace, fqdnActive)
}

// RenderSandboxEgressNetworkPolicyWithName renders the baseline sandbox egress NetworkPolicy with a specific name.
// All v1-defaulted fields (such as port protocols) are explicitly populated.
func RenderSandboxEgressNetworkPolicyWithName(name, namespace string, fqdnActive bool) *networkingv1.NetworkPolicy {
	if name == "" {
		name = DefaultSandboxEgressNetworkPolicyName
	}
	if namespace == "" {
		namespace = DefaultSandboxNamespace
	}

	tcpProto := corev1.ProtocolTCP
	udpProto := corev1.ProtocolUDP

	port53 := intstr.FromInt32(53)
	port80 := intstr.FromInt32(80)
	port443 := intstr.FromInt32(443)
	port9418 := intstr.FromInt32(9418)

	dnsRule := networkingv1.NetworkPolicyEgressRule{
		To: []networkingv1.NetworkPolicyPeer{
			{
				NamespaceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"kubernetes.io/metadata.name": "kube-system",
					},
				},
				PodSelector: &metav1.LabelSelector{
					MatchExpressions: []metav1.LabelSelectorRequirement{
						{
							Key:      "k8s-app",
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{"kube-dns", "node-local-dns"},
						},
					},
				},
			},
			{
				IPBlock: &networkingv1.IPBlock{
					CIDR: "169.254.20.10/32",
				},
			},
			{
				IPBlock: &networkingv1.IPBlock{
					CIDR: "169.254.169.254/32",
				},
			},
		},
		Ports: []networkingv1.NetworkPolicyPort{
			{
				Protocol: &udpProto,
				Port:     &port53,
			},
			{
				Protocol: &tcpProto,
				Port:     &port53,
			},
		},
	}

	internetRule := networkingv1.NetworkPolicyEgressRule{
		To: []networkingv1.NetworkPolicyPeer{
			{
				IPBlock: &networkingv1.IPBlock{
					CIDR: "0.0.0.0/0",
					Except: []string{
						"10.0.0.0/8",
						"172.16.0.0/12",
						"192.168.0.0/16",
						"100.64.0.0/10",
						"169.254.0.0/16",
					},
				},
			},
		},
		Ports: []networkingv1.NetworkPolicyPort{
			{
				Protocol: &tcpProto,
				Port:     &port80,
			},
			{
				Protocol: &tcpProto,
				Port:     &port443,
			},
			{
				Protocol: &tcpProto,
				Port:     &port9418,
			},
		},
	}

	gitServerRule := networkingv1.NetworkPolicyEgressRule{
		To: []networkingv1.NetworkPolicyPeer{
			{
				PodSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app": "git-server",
					},
				},
			},
		},
		Ports: []networkingv1.NetworkPolicyPort{
			{
				Protocol: &tcpProto,
				Port:     &port80,
			},
		},
	}

	var egressRules []networkingv1.NetworkPolicyEgressRule
	if fqdnActive {
		egressRules = []networkingv1.NetworkPolicyEgressRule{dnsRule, gitServerRule}
	} else {
		egressRules = []networkingv1.NetworkPolicyEgressRule{dnsRule, internetRule, gitServerRule}
	}

	return &networkingv1.NetworkPolicy{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "networking.k8s.io/v1",
			Kind:       "NetworkPolicy",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					LabelSandbox: "true",
				},
			},
			PolicyTypes: []networkingv1.PolicyType{
				networkingv1.PolicyTypeEgress,
			},
			Egress: egressRules,
		},
	}
}

// RenderServicePortalEgressNetworkPolicy renders the companion egress NetworkPolicy
// permitting pods opted into service-portal NRI interception to reach the local node
// proxy ports (UDP/TCP 1053 for DNS, TCP 8080 for HTTP, TCP 8443 for HTTPS) after
// host-side DNAT. Host-side PORTAL_EG_<iface> netfilter rules restrict intercepted
// pods exclusively to <nodeIP>:1053/:8080/:8443.
func RenderServicePortalEgressNetworkPolicy(namespace string) *networkingv1.NetworkPolicy {
	return RenderServicePortalEgressNetworkPolicyWithName(DefaultServicePortalEgressNetworkPolicyName, namespace)
}

// RenderServicePortalEgressNetworkPolicyWithName renders the companion service-portal
// egress NetworkPolicy with a specific name. All v1-defaulted fields are explicitly populated.
func RenderServicePortalEgressNetworkPolicyWithName(name, namespace string) *networkingv1.NetworkPolicy {
	if name == "" {
		name = DefaultServicePortalEgressNetworkPolicyName
	}
	if namespace == "" {
		namespace = DefaultSandboxNamespace
	}

	tcpProto := corev1.ProtocolTCP
	udpProto := corev1.ProtocolUDP

	port1053 := intstr.FromInt32(1053)
	port8080 := intstr.FromInt32(8080)
	port8443 := intstr.FromInt32(8443)

	return &networkingv1.NetworkPolicy{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "networking.k8s.io/v1",
			Kind:       "NetworkPolicy",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					LabelServicePortalIntercept: "true",
				},
			},
			PolicyTypes: []networkingv1.PolicyType{
				networkingv1.PolicyTypeEgress,
			},
			Egress: []networkingv1.NetworkPolicyEgressRule{
				{
					Ports: []networkingv1.NetworkPolicyPort{
						{
							Protocol: &udpProto,
							Port:     &port1053,
						},
						{
							Protocol: &tcpProto,
							Port:     &port1053,
						},
						{
							Protocol: &tcpProto,
							Port:     &port8080,
						},
						{
							Protocol: &tcpProto,
							Port:     &port8443,
						},
					},
				},
			},
		},
	}
}

// RenderRegistryServicePortalEgressNetworkPolicy renders the companion egress NetworkPolicy
// permitting sandbox pods to reach the in-cluster OCI registry service portal on TCP port 5000
// in servicePortalsNamespace.
func RenderRegistryServicePortalEgressNetworkPolicy(namespace, servicePortalsNamespace string) *networkingv1.NetworkPolicy {
	return RenderRegistryServicePortalEgressNetworkPolicyWithName(
		DefaultRegistryServicePortalEgressNetworkPolicyName,
		namespace,
		servicePortalsNamespace,
	)
}

// RenderRegistryServicePortalEgressNetworkPolicyWithName renders the companion registry service
// portal egress NetworkPolicy with a specific name. All v1-defaulted fields are explicitly populated.
func RenderRegistryServicePortalEgressNetworkPolicyWithName(name, namespace, servicePortalsNamespace string) *networkingv1.NetworkPolicy {
	if name == "" {
		name = DefaultRegistryServicePortalEgressNetworkPolicyName
	}
	if namespace == "" {
		namespace = DefaultSandboxNamespace
	}
	trimmedServicePortalsNamespace := strings.TrimSpace(servicePortalsNamespace)
	if trimmedServicePortalsNamespace == "" {
		trimmedServicePortalsNamespace = DefaultServicePortalsNamespace
	}

	tcpProtocol := corev1.ProtocolTCP
	registryPort := intstr.FromInt32(DefaultRegistryServicePortalPort)

	return &networkingv1.NetworkPolicy{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "networking.k8s.io/v1",
			Kind:       "NetworkPolicy",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					LabelSandbox: "true",
				},
			},
			PolicyTypes: []networkingv1.PolicyType{
				networkingv1.PolicyTypeEgress,
			},
			Egress: []networkingv1.NetworkPolicyEgressRule{
				{
					To: []networkingv1.NetworkPolicyPeer{
						{
							NamespaceSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									"kubernetes.io/metadata.name": trimmedServicePortalsNamespace,
								},
							},
						},
					},
					Ports: []networkingv1.NetworkPolicyPort{
						{
							Protocol: &tcpProtocol,
							Port:     &registryPort,
						},
					},
				},
			},
		},
	}
}
