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
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	portalsv1 "github.com/gke-labs/service-portals/pkg/portals/v1"
	"golang.org/x/oauth2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestBuildServicePortalEgressPolicy(t *testing.T) {
	testCases := []struct {
		name           string
		options        ServicePortalOptions
		wantHTTPHosts  []string
		wantHTTPSHosts []string
	}{
		{
			name:           "empty hosts produces empty policy",
			options:        ServicePortalOptions{},
			wantHTTPHosts:  nil,
			wantHTTPSHosts: nil,
		},
		{
			name: "only https hosts produces https rule only",
			options: ServicePortalOptions{
				AllowedHosts: []string{"*.googleapis.com", "github.com"},
			},
			wantHTTPHosts:  nil,
			wantHTTPSHosts: []string{"*.googleapis.com", "github.com"},
		},
		{
			name: "only http hosts produces http rule only",
			options: ServicePortalOptions{
				AllowedHTTPHosts: []string{"deb.debian.org", "archive.ubuntu.com"},
			},
			wantHTTPHosts:  []string{"deb.debian.org", "archive.ubuntu.com"},
			wantHTTPSHosts: nil,
		},
		{
			name: "both http and https hosts configure their respective rules",
			options: ServicePortalOptions{
				AllowedHosts:     []string{"*.googleapis.com", "github.com"},
				AllowedHTTPHosts: []string{"deb.debian.org", "archive.ubuntu.com"},
			},
			wantHTTPHosts:  []string{"deb.debian.org", "archive.ubuntu.com"},
			wantHTTPSHosts: []string{"*.googleapis.com", "github.com"},
		},
		{
			name: "defaults produce package mirrors for http and all default hosts for https",
			options: ServicePortalOptions{
				AllowedHosts:     DefaultServicePortalAllowedHosts,
				AllowedHTTPHosts: DefaultServicePortalAllowedHTTPHosts,
			},
			wantHTTPHosts:  DefaultServicePortalAllowedHTTPHosts,
			wantHTTPSHosts: DefaultServicePortalAllowedHosts,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			policy := BuildServicePortalEgressPolicy(tc.options)
			if err := policy.Validate(); err != nil {
				t.Fatalf("policy failed validation: %v", err)
			}
			var gotHTTPHosts []string
			var gotHTTPSHosts []string
			for _, rule := range policy.GetRules() {
				if rule.GetHttp() != nil {
					gotHTTPHosts = rule.GetHttp().GetHostnames()
				}
				if rule.GetHttps() != nil {
					gotHTTPSHosts = rule.GetHttps().GetHostnames()
				}
			}
			if !reflect.DeepEqual(gotHTTPHosts, tc.wantHTTPHosts) {
				t.Errorf("got HTTP hostnames %v, want %v", gotHTTPHosts, tc.wantHTTPHosts)
			}
			if !reflect.DeepEqual(gotHTTPSHosts, tc.wantHTTPSHosts) {
				t.Errorf("got HTTPS hostnames %v, want %v", gotHTTPSHosts, tc.wantHTTPSHosts)
			}
		})
	}
}

func TestBuildServicePortalEgressPolicy_GoProxyServicePortal(t *testing.T) {
	const defaultGoProxyBackendURL = "http://goproxy-service-portal.eval-runner-service-portals.svc:8080"

	t.Run("enabled with default namespace forwards proxy.golang.org when not in AllowedHosts", func(t *testing.T) {
		options := ServicePortalOptions{
			AllowedHosts:                []string{"github.com", "*.googleapis.com"},
			GoProxyServicePortalEnabled: true,
		}
		policy := BuildServicePortalEgressPolicyWithCredentials(options, nil, false)
		if err := policy.Validate(); err != nil {
			t.Fatalf("policy failed validation: %v", err)
		}

		goProxyRule := policy.Match(portalsv1.ProtocolHTTPS, "proxy.golang.org", 443)
		if goProxyRule == nil || goProxyRule.GetHttps() == nil {
			t.Fatalf("expected HTTPS rule matching proxy.golang.org, got %v", goProxyRule)
		}
		if got := goProxyRule.GetHttps().GetEffects().GetBackendUrl(); got != defaultGoProxyBackendURL {
			t.Fatalf("got BackendUrl=%q, want %q", got, defaultGoProxyBackendURL)
		}

		githubRule := policy.Match(portalsv1.ProtocolHTTPS, "github.com", 443)
		if githubRule == nil || githubRule.GetHttps() == nil {
			t.Fatalf("expected HTTPS rule matching github.com, got %v", githubRule)
		}
		if got := githubRule.GetHttps().GetEffects().GetBackendUrl(); got != "" {
			t.Fatalf("expected github.com rule without BackendUrl, got %q", got)
		}
	})

	t.Run("enabled deduplicates proxy.golang.org from AllowedHosts without validation ties", func(t *testing.T) {
		testCases := []struct {
			name         string
			allowedHosts []string
		}{
			{
				name:         "mixed with other allowed hosts",
				allowedHosts: []string{"github.com", "proxy.golang.org", "*.googleapis.com"},
			},
			{
				name:         "uppercase with trailing dot",
				allowedHosts: []string{"github.com", "PROXY.GOLANG.ORG."},
			},
			{
				name:         "only host in AllowedHosts",
				allowedHosts: []string{"proxy.golang.org"},
			},
			{
				name:         "empty AllowedHosts",
				allowedHosts: nil,
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				options := ServicePortalOptions{
					AllowedHosts:                testCase.allowedHosts,
					GoProxyServicePortalEnabled: true,
				}
				policy := BuildServicePortalEgressPolicyWithCredentials(options, nil, false)
				if err := policy.Validate(); err != nil {
					t.Fatalf("policy failed validation: %v", err)
				}
				goProxyRule := policy.Match(portalsv1.ProtocolHTTPS, "proxy.golang.org", 443)
				if goProxyRule == nil || goProxyRule.GetHttps() == nil {
					t.Fatalf("expected HTTPS rule matching proxy.golang.org, got %v", goProxyRule)
				}
				if got := goProxyRule.GetHttps().GetEffects().GetBackendUrl(); got != defaultGoProxyBackendURL {
					t.Fatalf("got BackendUrl=%q, want %q", got, defaultGoProxyBackendURL)
				}
			})
		}
	})

	t.Run("enabled respects custom ServicePortalsNamespace", func(t *testing.T) {
		options := ServicePortalOptions{
			AllowedHosts:                []string{"github.com", "proxy.golang.org"},
			GoProxyServicePortalEnabled: true,
			ServicePortalsNamespace:     "custom-service-portals",
		}
		policy := BuildServicePortalEgressPolicyWithCredentials(options, nil, false)
		if err := policy.Validate(); err != nil {
			t.Fatalf("policy failed validation: %v", err)
		}

		wantBackendURL := "http://goproxy-service-portal.custom-service-portals.svc:8080"
		goProxyRule := policy.Match(portalsv1.ProtocolHTTPS, "proxy.golang.org", 443)
		if goProxyRule == nil || goProxyRule.GetHttps() == nil {
			t.Fatalf("expected HTTPS rule matching proxy.golang.org, got %v", goProxyRule)
		}
		if got := goProxyRule.GetHttps().GetEffects().GetBackendUrl(); got != wantBackendURL {
			t.Fatalf("got BackendUrl=%q, want %q", got, wantBackendURL)
		}
	})

	t.Run("disabled leaves behavior unchanged", func(t *testing.T) {
		optionsWithHost := ServicePortalOptions{
			AllowedHosts:                []string{"github.com", "proxy.golang.org"},
			GoProxyServicePortalEnabled: false,
		}
		policyWithHost := BuildServicePortalEgressPolicyWithCredentials(optionsWithHost, nil, false)
		if err := policyWithHost.Validate(); err != nil {
			t.Fatalf("policy failed validation: %v", err)
		}
		goProxyRule := policyWithHost.Match(portalsv1.ProtocolHTTPS, "proxy.golang.org", 443)
		if goProxyRule == nil || goProxyRule.GetHttps() == nil {
			t.Fatalf("expected plain HTTPS rule matching proxy.golang.org, got %v", goProxyRule)
		}
		if goProxyRule.GetHttps().GetEffects() != nil {
			t.Fatalf("expected no effects when GoProxyServicePortalEnabled is false, got %+v", goProxyRule.GetHttps().GetEffects())
		}

		optionsWithoutHost := ServicePortalOptions{
			AllowedHosts:                []string{"github.com"},
			GoProxyServicePortalEnabled: false,
		}
		policyWithoutHost := BuildServicePortalEgressPolicyWithCredentials(optionsWithoutHost, nil, false)
		if err := policyWithoutHost.Validate(); err != nil {
			t.Fatalf("policy failed validation: %v", err)
		}
		if matched := policyWithoutHost.Match(portalsv1.ProtocolHTTPS, "proxy.golang.org", 443); matched != nil {
			t.Fatalf("expected proxy.golang.org not to match when absent from AllowedHosts and disabled, got %v", matched)
		}
	})

	t.Run("coexists with model credential and GCP access token rules", func(t *testing.T) {
		credential, ok, err := ModelCredentialForModel("google/gemini-2.5-flash", "")
		if err != nil || !ok {
			t.Fatalf("ModelCredentialForModel failed: ok=%v err=%v", ok, err)
		}
		options := ServicePortalOptions{
			AllowedHosts:                append([]string{"proxy.golang.org"}, DefaultServicePortalAllowedHosts...),
			AllowedHTTPHosts:            DefaultServicePortalAllowedHTTPHosts,
			GoProxyServicePortalEnabled: true,
			ServicePortalsNamespace:     "eval-runner-service-portals",
		}
		policy := BuildServicePortalEgressPolicyWithCredentials(options, &credential, true)
		if err := policy.Validate(); err != nil {
			t.Fatalf("policy failed validation: %v", err)
		}

		goProxyRule := policy.Match(portalsv1.ProtocolHTTPS, "proxy.golang.org", 443)
		if got := goProxyRule.GetHttps().GetEffects().GetBackendUrl(); got != defaultGoProxyBackendURL {
			t.Fatalf("got BackendUrl=%q, want %q", got, defaultGoProxyBackendURL)
		}
		modelRule := policy.Match(portalsv1.ProtocolHTTPS, "generativelanguage.googleapis.com", 443)
		if modelRule == nil || len(modelRule.GetHttps().GetEffects().GetReplaceHeaders()) != 1 {
			t.Fatalf("expected model credential rule, got %v", modelRule)
		}
		gcpRule := policy.Match(portalsv1.ProtocolHTTPS, "storage.googleapis.com", 443)
		if gcpRule == nil || len(gcpRule.GetHttps().GetEffects().GetReplaceHeaders()) != 1 {
			t.Fatalf("expected GCP credential rule, got %v", gcpRule)
		}
	})
}

func TestServicePortalOptions_RegistryServicePortalAddress(t *testing.T) {
	testCases := []struct {
		name        string
		options     ServicePortalOptions
		wantHost    string
		wantAddress string
	}{
		{
			name:        "disabled returns empty strings",
			options:     ServicePortalOptions{RegistryServicePortalEnabled: false},
			wantHost:    "",
			wantAddress: "",
		},
		{
			name:        "enabled with empty namespace uses default namespace",
			options:     ServicePortalOptions{RegistryServicePortalEnabled: true},
			wantHost:    "registry-service-portal.eval-runner-service-portals.svc",
			wantAddress: "registry-service-portal.eval-runner-service-portals.svc:5000",
		},
		{
			name: "enabled with whitespace namespace uses default namespace",
			options: ServicePortalOptions{
				RegistryServicePortalEnabled: true,
				ServicePortalsNamespace:      "   ",
			},
			wantHost:    "registry-service-portal.eval-runner-service-portals.svc",
			wantAddress: "registry-service-portal.eval-runner-service-portals.svc:5000",
		},
		{
			name: "enabled with custom namespace trims whitespace",
			options: ServicePortalOptions{
				RegistryServicePortalEnabled: true,
				ServicePortalsNamespace:      " custom-service-portals ",
			},
			wantHost:    "registry-service-portal.custom-service-portals.svc",
			wantAddress: "registry-service-portal.custom-service-portals.svc:5000",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.options.RegistryServicePortalHost(); got != testCase.wantHost {
				t.Errorf("RegistryServicePortalHost() = %q, want %q", got, testCase.wantHost)
			}
			if got := testCase.options.RegistryServicePortalAddress(); got != testCase.wantAddress {
				t.Errorf("RegistryServicePortalAddress() = %q, want %q", got, testCase.wantAddress)
			}
		})
	}
}

func TestBuildServicePortalEgressPolicy_RegistryServicePortal(t *testing.T) {
	const defaultRegistryHost = "registry-service-portal.eval-runner-service-portals.svc"

	t.Run("enabled with default namespace allows HTTP on port 5000 only", func(t *testing.T) {
		options := ServicePortalOptions{
			AllowedHosts:                 []string{"github.com", "*.googleapis.com"},
			AllowedHTTPHosts:             []string{"deb.debian.org"},
			RegistryServicePortalEnabled: true,
		}
		policy := BuildServicePortalEgressPolicyWithCredentials(options, nil, false)
		if err := policy.Validate(); err != nil {
			t.Fatalf("policy failed validation: %v", err)
		}

		registryRule := policy.Match(portalsv1.ProtocolHTTP, defaultRegistryHost, 5000)
		if registryRule == nil || registryRule.GetHttp() == nil {
			t.Fatalf("expected HTTP rule matching %s:5000, got %v", defaultRegistryHost, registryRule)
		}
		if gotPorts := registryRule.GetHttp().GetPorts().GetNumbers(); !reflect.DeepEqual(gotPorts, []int32{5000}) {
			t.Fatalf("got ports %v, want [5000]", gotPorts)
		}
		if matchedPort80 := policy.Match(portalsv1.ProtocolHTTP, defaultRegistryHost, 80); matchedPort80 != nil {
			t.Fatalf("expected %s:80 not to match, got %v", defaultRegistryHost, matchedPort80)
		}
		if matchedHTTPS := policy.Match(portalsv1.ProtocolHTTPS, defaultRegistryHost, 5000); matchedHTTPS != nil {
			t.Fatalf("expected HTTPS %s:5000 not to match, got %v", defaultRegistryHost, matchedHTTPS)
		}
	})

	t.Run("enabled with empty allow lists still produces valid registry policy", func(t *testing.T) {
		options := ServicePortalOptions{
			RegistryServicePortalEnabled: true,
		}
		policy := BuildServicePortalEgressPolicyWithCredentials(options, nil, false)
		if err := policy.Validate(); err != nil {
			t.Fatalf("policy failed validation: %v", err)
		}
		registryRule := policy.Match(portalsv1.ProtocolHTTP, defaultRegistryHost, 5000)
		if registryRule == nil || registryRule.GetHttp() == nil {
			t.Fatalf("expected HTTP rule matching %s:5000, got %v", defaultRegistryHost, registryRule)
		}
	})

	t.Run("enabled respects custom ServicePortalsNamespace", func(t *testing.T) {
		options := ServicePortalOptions{
			AllowedHosts:                 []string{"github.com"},
			RegistryServicePortalEnabled: true,
			ServicePortalsNamespace:      "custom-service-portals",
		}
		policy := BuildServicePortalEgressPolicyWithCredentials(options, nil, false)
		if err := policy.Validate(); err != nil {
			t.Fatalf("policy failed validation: %v", err)
		}

		wantHost := "registry-service-portal.custom-service-portals.svc"
		registryRule := policy.Match(portalsv1.ProtocolHTTP, wantHost, 5000)
		if registryRule == nil || registryRule.GetHttp() == nil {
			t.Fatalf("expected HTTP rule matching %s:5000, got %v", wantHost, registryRule)
		}
		if matchedDefault := policy.Match(portalsv1.ProtocolHTTP, defaultRegistryHost, 5000); matchedDefault != nil {
			t.Fatalf("expected default namespace host %s not to match when custom namespace is set, got %v", defaultRegistryHost, matchedDefault)
		}
	})

	t.Run("enabled coexists without ties when registry host is also in AllowedHTTPHosts on port 80", func(t *testing.T) {
		options := ServicePortalOptions{
			AllowedHTTPHosts:             []string{"deb.debian.org", defaultRegistryHost},
			RegistryServicePortalEnabled: true,
		}
		policy := BuildServicePortalEgressPolicyWithCredentials(options, nil, false)
		if err := policy.Validate(); err != nil {
			t.Fatalf("policy failed validation: %v", err)
		}
		if matched5000 := policy.Match(portalsv1.ProtocolHTTP, defaultRegistryHost, 5000); matched5000 == nil {
			t.Fatalf("expected HTTP match on port 5000")
		}
		if matched80 := policy.Match(portalsv1.ProtocolHTTP, defaultRegistryHost, 80); matched80 == nil {
			t.Fatalf("expected HTTP match on port 80")
		}
	})

	t.Run("disabled leaves policy unchanged without registry port 5000 rule", func(t *testing.T) {
		options := ServicePortalOptions{
			AllowedHosts:                 DefaultServicePortalAllowedHosts,
			AllowedHTTPHosts:             DefaultServicePortalAllowedHTTPHosts,
			RegistryServicePortalEnabled: false,
		}
		policy := BuildServicePortalEgressPolicyWithCredentials(options, nil, false)
		if err := policy.Validate(); err != nil {
			t.Fatalf("policy failed validation: %v", err)
		}
		if matched := policy.Match(portalsv1.ProtocolHTTP, defaultRegistryHost, 5000); matched != nil {
			t.Fatalf("expected %s:5000 not to match when disabled, got %v", defaultRegistryHost, matched)
		}
	})

	t.Run("coexists with goproxy, model credential, and GCP access token rules", func(t *testing.T) {
		credential, ok, err := ModelCredentialForModel("google/gemini-2.5-flash", "")
		if err != nil || !ok {
			t.Fatalf("ModelCredentialForModel failed: ok=%v err=%v", ok, err)
		}
		options := ServicePortalOptions{
			AllowedHosts:                 append([]string{"proxy.golang.org"}, DefaultServicePortalAllowedHosts...),
			AllowedHTTPHosts:             DefaultServicePortalAllowedHTTPHosts,
			GoProxyServicePortalEnabled:  true,
			RegistryServicePortalEnabled: true,
			ServicePortalsNamespace:      "eval-runner-service-portals",
		}
		policy := BuildServicePortalEgressPolicyWithCredentials(options, &credential, true)
		if err := policy.Validate(); err != nil {
			t.Fatalf("policy failed validation: %v", err)
		}

		registryRule := policy.Match(portalsv1.ProtocolHTTP, defaultRegistryHost, 5000)
		if registryRule == nil || registryRule.GetHttp() == nil {
			t.Fatalf("expected HTTP registry rule matching %s:5000, got %v", defaultRegistryHost, registryRule)
		}
		goProxyRule := policy.Match(portalsv1.ProtocolHTTPS, "proxy.golang.org", 443)
		if goProxyRule == nil || goProxyRule.GetHttps().GetEffects().GetBackendUrl() == "" {
			t.Fatalf("expected Go proxy rule with BackendUrl, got %v", goProxyRule)
		}
		modelRule := policy.Match(portalsv1.ProtocolHTTPS, "generativelanguage.googleapis.com", 443)
		if modelRule == nil || len(modelRule.GetHttps().GetEffects().GetReplaceHeaders()) != 1 {
			t.Fatalf("expected model credential rule, got %v", modelRule)
		}
		gcpRule := policy.Match(portalsv1.ProtocolHTTPS, "storage.googleapis.com", 443)
		if gcpRule == nil || len(gcpRule.GetHttps().GetEffects().GetReplaceHeaders()) != 1 {
			t.Fatalf("expected GCP credential rule, got %v", gcpRule)
		}
	})
}

func TestModelCredentialForModel(t *testing.T) {
	testCases := []struct {
		name         string
		model        string
		modelBaseURL string
		wantOK       bool
		wantErr      bool
		wantHost     string
		wantProtocol string
		wantHeader   string
		wantPrefix   string
		wantKey      string
		wantSecret   string
	}{
		{
			name:   "mock model has no credential",
			model:  "mockllm/model",
			wantOK: false,
		},
		{
			name:         "google gemini model",
			model:        "google/gemini-2.5-flash",
			wantOK:       true,
			wantHost:     "generativelanguage.googleapis.com",
			wantProtocol: "https",
			wantHeader:   "x-goog-api-key",
			wantKey:      "GEMINI_API_KEY",
			wantSecret:   "gemini-api-key",
		},
		{
			name:         "openai model",
			model:        "openai/gpt-4o",
			wantOK:       true,
			wantHost:     "api.openai.com",
			wantProtocol: "https",
			wantHeader:   "Authorization",
			wantPrefix:   "Bearer ",
			wantKey:      "OPENAI_API_KEY",
			wantSecret:   "openai-api-key",
		},
		{
			name:         "anthropic model",
			model:        "anthropic/claude-sonnet-4-5",
			wantOK:       true,
			wantHost:     "api.anthropic.com",
			wantProtocol: "https",
			wantHeader:   "x-api-key",
			wantKey:      "ANTHROPIC_API_KEY",
			wantSecret:   "anthropic-api-key",
		},
		{
			name:         "custom http stand-in model base URL overrides host and protocol",
			model:        "google/gemini-3.5-flash-lite",
			modelBaseURL: "http://model-standin.eval-runner-sandbox.svc.cluster.local",
			wantOK:       true,
			wantHost:     "model-standin.eval-runner-sandbox.svc.cluster.local",
			wantProtocol: "http",
			wantHeader:   "x-goog-api-key",
			wantKey:      "GEMINI_API_KEY",
			wantSecret:   "gemini-api-key",
		},
		{
			name:         "invalid model base URL returns error",
			model:        "google/gemini-2.5-flash",
			modelBaseURL: "ftp://invalid.example.com",
			wantErr:      true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			credential, ok, err := ModelCredentialForModel(testCase.model, testCase.modelBaseURL)
			if testCase.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok != testCase.wantOK {
				t.Fatalf("got ok=%v, want %v", ok, testCase.wantOK)
			}
			if !ok {
				return
			}
			if credential.Host != testCase.wantHost ||
				credential.Protocol != testCase.wantProtocol ||
				credential.Header != testCase.wantHeader ||
				credential.Prefix != testCase.wantPrefix ||
				credential.SecretKey != testCase.wantKey ||
				credential.CredentialName != testCase.wantSecret {
				t.Fatalf("got credential %+v, want host=%s protocol=%s header=%s prefix=%q key=%s secret=%s",
					credential, testCase.wantHost, testCase.wantProtocol, testCase.wantHeader, testCase.wantPrefix, testCase.wantKey, testCase.wantSecret)
			}
		})
	}
}

func TestBuildServicePortalEgressPolicyWithCredential(t *testing.T) {
	realKey := "super-secret-model-api-key"
	resolveSecret := func(name string) ([]byte, error) {
		if name == "gemini-api-key" || name == "openai-api-key" {
			return []byte(realKey), nil
		}
		return nil, nil
	}

	t.Run("gemini exact rule overrides wildcard and replaces header only when present", func(t *testing.T) {
		credential, ok, err := ModelCredentialForModel("google/gemini-2.5-flash", "")
		if err != nil || !ok {
			t.Fatalf("ModelCredentialForModel failed: ok=%v err=%v", ok, err)
		}
		options := ServicePortalOptions{
			AllowedHosts:     append([]string{"generativelanguage.googleapis.com"}, DefaultServicePortalAllowedHosts...),
			AllowedHTTPHosts: DefaultServicePortalAllowedHTTPHosts,
		}
		policy := BuildServicePortalEgressPolicyWithCredential(options, &credential)
		if err := policy.Validate(); err != nil {
			t.Fatalf("policy failed upstream validation (tie check): %v", err)
		}

		// Model host with placeholder header -> replaced with real key.
		matchedRule := policy.Match(portalsv1.ProtocolHTTPS, "generativelanguage.googleapis.com", 443)
		if matchedRule == nil {
			t.Fatalf("expected policy to match generativelanguage.googleapis.com")
		}
		headersWithKey := http.Header{"X-Goog-Api-Key": []string{"placeholder-model-credential"}}
		if err := matchedRule.ApplyEffects(headersWithKey, resolveSecret); err != nil {
			t.Fatalf("ApplyEffects failed: %v", err)
		}
		if got := headersWithKey.Get("x-goog-api-key"); got != realKey {
			t.Fatalf("got x-goog-api-key=%q, want real key", got)
		}

		// Model host WITHOUT header -> forwarded without credential.
		headersWithoutKey := http.Header{"Content-Type": []string{"application/json"}}
		if err := matchedRule.ApplyEffects(headersWithoutKey, resolveSecret); err != nil {
			t.Fatalf("ApplyEffects without header failed: %v", err)
		}
		if got := headersWithoutKey.Get("x-goog-api-key"); got != "" {
			t.Fatalf("expected x-goog-api-key to remain absent, got %q", got)
		}

		// Another host (*.googleapis.com) -> forwarded without credential even if header is sent.
		otherRule := policy.Match(portalsv1.ProtocolHTTPS, "storage.googleapis.com", 443)
		if otherRule == nil {
			t.Fatalf("expected policy to match storage.googleapis.com")
		}
		otherHeaders := http.Header{"X-Goog-Api-Key": []string{"placeholder-model-credential"}}
		if err := otherRule.ApplyEffects(otherHeaders, resolveSecret); err != nil {
			t.Fatalf("ApplyEffects on other host failed: %v", err)
		}
		if got := otherHeaders.Get("x-goog-api-key"); got != "placeholder-model-credential" {
			t.Fatalf("expected other host header untouched, got %q", got)
		}
	})

	t.Run("openai rule prepends Bearer prefix", func(t *testing.T) {
		credential, ok, err := ModelCredentialForModel("openai/gpt-4o", "")
		if err != nil || !ok {
			t.Fatalf("ModelCredentialForModel failed: ok=%v err=%v", ok, err)
		}
		policy := BuildServicePortalEgressPolicyWithCredential(ServicePortalOptions{
			AllowedHosts: DefaultServicePortalAllowedHosts,
		}, &credential)
		if err := policy.Validate(); err != nil {
			t.Fatalf("policy failed upstream validation: %v", err)
		}
		matchedRule := policy.Match(portalsv1.ProtocolHTTPS, "api.openai.com", 443)
		if matchedRule == nil {
			t.Fatalf("expected policy to match api.openai.com")
		}
		headers := http.Header{"Authorization": []string{"Bearer placeholder-model-credential"}}
		if err := matchedRule.ApplyEffects(headers, resolveSecret); err != nil {
			t.Fatalf("ApplyEffects failed: %v", err)
		}
		if got := headers.Get("Authorization"); got != "Bearer "+realKey {
			t.Fatalf("got Authorization=%q, want Bearer <key>", got)
		}
	})

	t.Run("custom http stand-in deduplicates against AllowedHTTPHosts", func(t *testing.T) {
		credential, ok, err := ModelCredentialForModel("google/gemini-3.5-flash-lite", "http://model-standin.eval-runner-sandbox.svc.cluster.local")
		if err != nil || !ok {
			t.Fatalf("ModelCredentialForModel failed: ok=%v err=%v", ok, err)
		}
		options := ServicePortalOptions{
			AllowedHTTPHosts: []string{"model-standin.eval-runner-sandbox.svc.cluster.local", "other-standin.eval-runner-sandbox.svc.cluster.local"},
		}
		policy := BuildServicePortalEgressPolicyWithCredential(options, &credential)
		if err := policy.Validate(); err != nil {
			t.Fatalf("policy failed upstream validation: %v", err)
		}
		standinRule := policy.Match(portalsv1.ProtocolHTTP, "model-standin.eval-runner-sandbox.svc.cluster.local", 80)
		if standinRule == nil || standinRule.GetHttp().GetEffects() == nil {
			t.Fatalf("expected HTTP credential rule for stand-in host")
		}
		otherRule := policy.Match(portalsv1.ProtocolHTTP, "other-standin.eval-runner-sandbox.svc.cluster.local", 80)
		if otherRule == nil || otherRule.GetHttp().GetEffects() != nil {
			t.Fatalf("expected plain HTTP rule without effects for other host")
		}
	})
}

func TestPrepareServicePortalBinding(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme corev1: %v", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme v1alpha1: %v", err)
	}

	controllerSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultModelCredentialsSecretName,
			Namespace: "eval-runner",
		},
		Data: map[string][]byte{
			"GEMINI_API_KEY": []byte("real-controller-gemini-key\n"),
		},
	}
	sandboxSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultModelCredentialsSecretName,
			Namespace: "eval-runner-sandbox",
		},
		Data: map[string][]byte{
			"GEMINI_API_KEY": []byte("wrong-sandbox-namespace-key"),
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(controllerSecret, sandboxSecret).Build()
	reconciler := &RunReconciler{
		Client:              fakeClient,
		APIReader:           fakeClient,
		ControllerNamespace: "eval-runner",
	}
	options := ServicePortalOptions{
		AllowedHosts:     DefaultServicePortalAllowedHosts,
		AllowedHTTPHosts: DefaultServicePortalAllowedHTTPHosts,
	}

	t.Run("sample-runner real model injects secret from controller namespace", func(t *testing.T) {
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "eval-runner"},
			Spec:       v1alpha1.RunSpec{Model: "google/gemini-2.5-flash"},
		}
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{{ID: "docs/comprehension", Spec: "evals/docs.py"}},
		}
		policy, secrets, credential, err := reconciler.prepareServicePortalBinding(t.Context(), run, manifest, "docs/comprehension", options)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if credential == nil || credential.Host != "generativelanguage.googleapis.com" {
			t.Fatalf("expected gemini credential, got %+v", credential)
		}
		if got := string(secrets["gemini-api-key"]); got != "real-controller-gemini-key" {
			t.Fatalf("expected trimmed controller secret, got %q", got)
		}
		if err := policy.Validate(); err != nil {
			t.Fatalf("generated policy invalid: %v", err)
		}
	})

	t.Run("sandbox task with real model receives model credential via service portal", func(t *testing.T) {
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "run-2", Namespace: "eval-runner"},
			Spec:       v1alpha1.RunSpec{Model: "google/gemini-2.5-flash"},
		}
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{
					ID: "terminal-bench/task",
					Resources: []v1alpha1.ManifestResource{
						{Type: v1alpha1.ResourceTypeSandbox},
					},
				},
			},
		}
		policy, secrets, credential, err := reconciler.prepareServicePortalBinding(t.Context(), run, manifest, "terminal-bench/task", options)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if credential == nil || credential.Host != "generativelanguage.googleapis.com" {
			t.Fatalf("expected gemini credential for sandbox task, got %+v", credential)
		}
		if got := string(secrets["gemini-api-key"]); got != "real-controller-gemini-key" {
			t.Fatalf("expected controller model secret, got %q", got)
		}
		if err := policy.Validate(); err != nil {
			t.Fatalf("generated policy invalid: %v", err)
		}
	})

	t.Run("sandbox task with gcp-project emits both generativelanguage and wildcard googleapis rules using impersonated token", func(t *testing.T) {
		const controllerToken = "controller-broad-oauth-token"
		const impersonatedToken = "ya29.impersonated-pool-proj-1-token"
		minter := &fakeGCPAccessTokenMinter{
			tokensByProject: map[string]string{
				"pool-proj-1": impersonatedToken,
			},
		}
		gcpReconciler := &RunReconciler{
			Client:               fakeClient,
			APIReader:            fakeClient,
			ControllerNamespace:  "eval-runner",
			GCPTokenSource:       oauth2.StaticTokenSource(&oauth2.Token{AccessToken: controllerToken}),
			GCPAccessTokenMinter: minter,
		}
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "run-gcp-model", Namespace: "eval-runner"},
			Spec:       v1alpha1.RunSpec{Model: "google/gemini-2.5-flash"},
			Status: v1alpha1.RunStatus{
				Tasks: []v1alpha1.TaskStatus{
					{ID: "kube-agents/kind-cluster", GCPProject: "pool-proj-1"},
				},
			},
		}
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{
					ID: "kube-agents/kind-cluster",
					Resources: []v1alpha1.ManifestResource{
						{Type: v1alpha1.ResourceTypeSandbox},
						{Type: v1alpha1.ResourceTypeGCPProject},
					},
				},
			},
		}
		policy, secrets, credential, err := gcpReconciler.prepareServicePortalBinding(t.Context(), run, manifest, "kube-agents/kind-cluster", options)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := policy.Validate(); err != nil {
			t.Fatalf("generated policy invalid: %v", err)
		}
		if credential == nil || credential.Host != "generativelanguage.googleapis.com" {
			t.Fatalf("expected gemini credential, got %+v", credential)
		}
		if got := string(secrets["gemini-api-key"]); got != "real-controller-gemini-key" {
			t.Fatalf("gemini-api-key secret = %q, want real-controller-gemini-key", got)
		}
		if got := string(secrets["gcp-access-token"]); got != impersonatedToken {
			t.Fatalf("gcp-access-token secret = %q, want impersonated token %q", got, impersonatedToken)
		}
		if got := string(secrets["gcp-access-token"]); got == controllerToken {
			t.Fatalf("gcp-access-token must never use controller token")
		}
		if !reflect.DeepEqual(minter.requestedProjects, []string{"pool-proj-1"}) {
			t.Fatalf("minter requestedProjects = %v, want [pool-proj-1]", minter.requestedProjects)
		}

		modelRule := policy.Match(portalsv1.ProtocolHTTPS, "generativelanguage.googleapis.com", 443)
		if modelRule == nil || modelRule.GetHttps() == nil || modelRule.GetHttps().GetEffects() == nil {
			t.Fatalf("expected exact-host rule for generativelanguage.googleapis.com, got %v", modelRule)
		}
		modelHeaders := modelRule.GetHttps().GetEffects().GetReplaceHeaders()
		if len(modelHeaders) != 1 || modelHeaders[0].Header != "x-goog-api-key" || modelHeaders[0].CredentialUri != "secret://gemini-api-key" {
			t.Fatalf("expected exact-host generativelanguage.googleapis.com to inject gemini-api-key, got %+v", modelHeaders)
		}

		gcpRule := policy.Match(portalsv1.ProtocolHTTPS, "cloudresourcemanager.googleapis.com", 443)
		if gcpRule == nil || gcpRule.GetHttps() == nil || gcpRule.GetHttps().GetEffects() == nil {
			t.Fatalf("expected wildcard *.googleapis.com rule, got %v", gcpRule)
		}
		gcpHeaders := gcpRule.GetHttps().GetEffects().GetReplaceHeaders()
		if len(gcpHeaders) != 1 || gcpHeaders[0].Header != "Authorization" || gcpHeaders[0].Prefix != "Bearer " || gcpHeaders[0].CredentialUri != "secret://gcp-access-token" {
			t.Fatalf("expected *.googleapis.com to inject gcp-access-token, got %+v", gcpHeaders)
		}
	})

	t.Run("gcp-project task fails closed when task GCPProject is empty", func(t *testing.T) {
		minter := &fakeGCPAccessTokenMinter{
			tokensByProject: map[string]string{
				"pool-proj-1": "ya29.impersonated-token",
			},
		}
		gcpReconciler := &RunReconciler{
			Client:               fakeClient,
			APIReader:            fakeClient,
			ControllerNamespace:  "eval-runner",
			GCPTokenSource:       oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "controller-broad-oauth-token"}),
			GCPAccessTokenMinter: minter,
		}
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "run-gcp-empty-project", Namespace: "eval-runner"},
			Spec:       v1alpha1.RunSpec{Model: "mockllm/model"},
			Status: v1alpha1.RunStatus{
				Tasks: []v1alpha1.TaskStatus{
					{ID: "kube-agents/kind-cluster", GCPProject: ""},
				},
			},
		}
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{
					ID: "kube-agents/kind-cluster",
					Resources: []v1alpha1.ManifestResource{
						{Type: v1alpha1.ResourceTypeSandbox},
						{Type: v1alpha1.ResourceTypeGCPProject},
					},
				},
			},
		}
		policy, secrets, _, err := gcpReconciler.prepareServicePortalBinding(t.Context(), run, manifest, "kube-agents/kind-cluster", options)
		if err == nil {
			t.Fatal("expected error when task GCPProject is empty")
		}
		if policy != nil || secrets != nil {
			t.Fatalf("expected nil policy and secrets on fail-closed error, got policy=%v secrets=%v", policy, secrets)
		}
		if len(minter.requestedProjects) != 0 {
			t.Fatalf("expected minter not called when task GCPProject is empty, got %v", minter.requestedProjects)
		}
	})

	t.Run("gcp-project task fails closed when impersonation fails and redacts tokens", func(t *testing.T) {
		const leakedToken = "ya29.secret-impersonation-failure-token"
		minter := &fakeGCPAccessTokenMinter{
			mintErr: errors.New("iamcredentials denied request with token " + leakedToken),
		}
		gcpReconciler := &RunReconciler{
			Client:               fakeClient,
			APIReader:            fakeClient,
			ControllerNamespace:  "eval-runner",
			GCPTokenSource:       oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "controller-broad-oauth-token"}),
			GCPAccessTokenMinter: minter,
		}
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "run-gcp-impersonation-fail", Namespace: "eval-runner"},
			Spec:       v1alpha1.RunSpec{Model: "mockllm/model"},
			Status: v1alpha1.RunStatus{
				Tasks: []v1alpha1.TaskStatus{
					{ID: "kube-agents/kind-cluster", GCPProject: "pool-proj-1"},
				},
			},
		}
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{
					ID: "kube-agents/kind-cluster",
					Resources: []v1alpha1.ManifestResource{
						{Type: v1alpha1.ResourceTypeSandbox},
						{Type: v1alpha1.ResourceTypeGCPProject},
					},
				},
			},
		}
		policy, secrets, _, err := gcpReconciler.prepareServicePortalBinding(t.Context(), run, manifest, "kube-agents/kind-cluster", options)
		if err == nil {
			t.Fatal("expected error when impersonation fails")
		}
		if policy != nil || secrets != nil {
			t.Fatalf("expected nil policy and secrets on impersonation failure, got policy=%v secrets=%v", policy, secrets)
		}
		if strings.Contains(err.Error(), leakedToken) {
			t.Fatalf("expected token redacted from impersonation error, got %q", err.Error())
		}
		if !strings.Contains(err.Error(), "[REDACTED]") {
			t.Fatalf("expected [REDACTED] in impersonation error, got %q", err.Error())
		}
	})

	t.Run("mock model never receives model credential", func(t *testing.T) {
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "run-3", Namespace: "eval-runner"},
			Spec:       v1alpha1.RunSpec{Model: "mockllm/model"},
		}
		_, secrets, credential, err := reconciler.prepareServicePortalBinding(t.Context(), run, nil, "docs/comprehension", options)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if credential != nil || secrets != nil {
			t.Fatalf("expected no credential for mock model")
		}
	})

	t.Run("missing secret binds base policy without error", func(t *testing.T) {
		emptyClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		emptyReconciler := &RunReconciler{
			Client:              emptyClient,
			APIReader:           emptyClient,
			ControllerNamespace: "eval-runner",
		}
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "run-4", Namespace: "eval-runner"},
			Spec:       v1alpha1.RunSpec{Model: "google/gemini-2.5-flash"},
		}
		policy, secrets, credential, err := emptyReconciler.prepareServicePortalBinding(t.Context(), run, nil, "docs/comprehension", options)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if credential != nil || secrets != nil {
			t.Fatalf("expected nil credential and secrets when Secret absent")
		}
		if err := policy.Validate(); err != nil {
			t.Fatalf("generated policy invalid: %v", err)
		}
	})

	t.Run("invalid header characters in secret fail without leaking secret value", func(t *testing.T) {
		badValue := "secret-part-one\nsecret-part-two"
		badSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      DefaultModelCredentialsSecretName,
				Namespace: "eval-runner",
			},
			Data: map[string][]byte{
				"GEMINI_API_KEY": []byte(badValue),
			},
		}
		badClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(badSecret).Build()
		badReconciler := &RunReconciler{
			Client:              badClient,
			APIReader:           badClient,
			ControllerNamespace: "eval-runner",
		}
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "run-5", Namespace: "eval-runner"},
			Spec:       v1alpha1.RunSpec{Model: "google/gemini-2.5-flash"},
		}
		_, _, _, err := badReconciler.prepareServicePortalBinding(t.Context(), run, nil, "docs/comprehension", options)
		if err == nil {
			t.Fatalf("expected error for invalid header characters")
		}
		errText := err.Error()
		if !strings.Contains(errText, "generativelanguage.googleapis.com") || !strings.Contains(errText, "x-goog-api-key") {
			t.Errorf("expected error to name host and header, got %q", errText)
		}
		if strings.Contains(errText, "secret-part-one") || strings.Contains(errText, "secret-part-two") {
			t.Errorf("error must never leak secret value, got %q", errText)
		}
	})
}

func TestRedactSecrets(t *testing.T) {
	secrets := map[string][]byte{"gemini-api-key": []byte("very-sensitive-key")}
	got := redactSecrets("upstream rejected very-sensitive-key on bind", secrets)
	if strings.Contains(got, "very-sensitive-key") {
		t.Fatalf("expected secret redacted, got %q", got)
	}
	if !strings.Contains(got, "[REDACTED]") {
		t.Fatalf("expected [REDACTED] placeholder, got %q", got)
	}
}
