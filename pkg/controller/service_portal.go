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
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/discovery"
	"github.com/gke-labs/eval-runner/pkg/modelclient/gemini"
	serviceportalclient "github.com/gke-labs/service-portals/pkg/client"
	portalsv1 "github.com/gke-labs/service-portals/pkg/portals/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	// DefaultModelCredentialsSecretName is the Secret in the controller's namespace
	// holding model API keys for agent tasks and service portal credential injection.
	DefaultModelCredentialsSecretName = "model-credentials"

	// DefaultServicePortalsNamespace is the default Kubernetes namespace where
	// ecosystem caching service portals run.
	DefaultServicePortalsNamespace = discovery.DefaultServicePortalsNamespace

	// DefaultRegistryServicePortalServiceName is the Kubernetes Service name for the
	// in-cluster OCI registry service portal.
	DefaultRegistryServicePortalServiceName = "registry-service-portal"

	// DefaultRegistryServicePortalPort is the TCP port for the in-cluster OCI registry service portal.
	DefaultRegistryServicePortalPort = discovery.DefaultRegistryServicePortalPort

	servicePortalCredentialURIScheme = "secret://"

	gcpAccessTokenHost           = "*.googleapis.com"
	gcpAccessTokenHeader         = "Authorization"
	gcpAccessTokenPrefix         = "Bearer "
	gcpAccessTokenCredentialName = "gcp-access-token"
)

// ServicePortalClient binds egress policies and companion secrets to sandbox pods.
type ServicePortalClient interface {
	Bind(ctx context.Context, podUID string, policy *portalsv1.EgressPolicy, secrets map[string][]byte) error
}

// DefaultServicePortalAllowedHosts defines the default allowed hostnames for task sandboxes behind the service portal:
// - Model APIs (*.googleapis.com)
// - Git hosts (github.com, *.github.com, *.githubusercontent.com)
// - Python package repositories and tokenizer assets (pypi.org, *.pypi.org, files.pythonhosted.org, *.pythonhosted.org, openaipublic.blob.core.windows.net)
// - Tool and package distributors for Terminal-Bench tasks (astral.sh, *.astral.sh, deb.debian.org, security.debian.org, *.debian.org, archive.ubuntu.com, security.ubuntu.com, ports.ubuntu.com, *.ubuntu.com)
var DefaultServicePortalAllowedHosts = []string{
	"*.googleapis.com",
	"github.com",
	"*.github.com",
	"*.githubusercontent.com",
	"pypi.org",
	"*.pypi.org",
	"files.pythonhosted.org",
	"*.pythonhosted.org",
	"openaipublic.blob.core.windows.net",
	"astral.sh",
	"*.astral.sh",
	"deb.debian.org",
	"security.debian.org",
	"*.debian.org",
	"archive.ubuntu.com",
	"security.ubuntu.com",
	"ports.ubuntu.com",
	"*.ubuntu.com",
}

// DefaultServicePortalAllowedHTTPHosts defines the default allowed hostnames for task sandboxes over cleartext HTTP behind the service portal:
// - Debian package mirrors (deb.debian.org, security.debian.org, *.debian.org)
// - Ubuntu package mirrors (archive.ubuntu.com, security.ubuntu.com, ports.ubuntu.com, *.ubuntu.com)
var DefaultServicePortalAllowedHTTPHosts = []string{
	"deb.debian.org",
	"security.debian.org",
	"*.debian.org",
	"archive.ubuntu.com",
	"security.ubuntu.com",
	"ports.ubuntu.com",
	"*.ubuntu.com",
}

// ServicePortalOptions configures settings for the Service Portal Controller and allowed egress hosts.
type ServicePortalOptions struct {
	// ControllerAddress is the host:port address of the Service Portal Controller.
	ControllerAddress string

	// CAFile is the file path to the CA certificate verifying the controller's TLS certificate.
	CAFile string

	// TokenFile is the file path to the projected service account token.
	TokenFile string

	// AllowedHosts is the list of allowed FQDN egress hostnames for the service portal.
	AllowedHosts []string

	// AllowedHTTPHosts is the list of allowed cleartext HTTP egress hostnames for the service portal.
	AllowedHTTPHosts []string

	// ModelBaseURL optionally overrides the model API base URL for sample-runner tasks
	// and targets the service portal credential replacement rule at that endpoint.
	ModelBaseURL string

	// GoProxyServicePortalEnabled forwards proxy.golang.org HTTPS traffic to the in-cluster Go proxy service portal.
	GoProxyServicePortalEnabled bool

	// RegistryServicePortalEnabled enables the in-cluster OCI registry service portal for task sandboxes.
	RegistryServicePortalEnabled bool

	// ServicePortalsNamespace is the Kubernetes namespace where ecosystem caching service portals run.
	// Defaults to DefaultServicePortalsNamespace when empty.
	ServicePortalsNamespace string
}

// EffectiveServicePortalsNamespace returns the normalized Kubernetes namespace where
// ecosystem service portals run, defaulting to DefaultServicePortalsNamespace when empty.
func (options ServicePortalOptions) EffectiveServicePortalsNamespace() string {
	trimmedNamespace := strings.TrimSpace(options.ServicePortalsNamespace)
	if trimmedNamespace == "" {
		return DefaultServicePortalsNamespace
	}
	return trimmedNamespace
}

// RegistryServicePortalHost returns the DNS hostname of the in-cluster OCI registry
// service portal (registry-service-portal.<namespace>.svc) when RegistryServicePortalEnabled
// is true, or "" when disabled.
func (options ServicePortalOptions) RegistryServicePortalHost() string {
	if !options.RegistryServicePortalEnabled {
		return ""
	}
	return fmt.Sprintf("%s.%s.svc", DefaultRegistryServicePortalServiceName, options.EffectiveServicePortalsNamespace())
}

// RegistryServicePortalAddress returns the host:port address of the in-cluster OCI registry
// service portal (registry-service-portal.<namespace>.svc:5000) when RegistryServicePortalEnabled
// is true, or "" when disabled.
func (options ServicePortalOptions) RegistryServicePortalAddress() string {
	host := options.RegistryServicePortalHost()
	if host == "" {
		return ""
	}
	return fmt.Sprintf("%s:%d", host, DefaultRegistryServicePortalPort)
}

type cachingServicePortal struct {
	enabled     func(options ServicePortalOptions) bool
	hostnames   []string
	serviceName string
	servicePort int32
}

var builtInCachingServicePortals = []cachingServicePortal{
	{
		enabled: func(options ServicePortalOptions) bool {
			return options.GoProxyServicePortalEnabled
		},
		hostnames:   []string{"proxy.golang.org"},
		serviceName: "goproxy-service-portal",
		servicePort: 8080,
	},
}

func (servicePortal cachingServicePortal) backendURL(namespace string) string {
	trimmedNamespace := strings.TrimSpace(namespace)
	if trimmedNamespace == "" {
		trimmedNamespace = DefaultServicePortalsNamespace
	}
	return fmt.Sprintf("http://%s.%s.svc:%d", servicePortal.serviceName, trimmedNamespace, servicePortal.servicePort)
}

// ServicePortalModelCredential describes how a model provider's API credential is
// injected on egress by the service portal.
type ServicePortalModelCredential struct {
	Host           string
	Protocol       string
	Port           int32
	Header         string
	Prefix         string
	SecretKey      string
	CredentialName string
}

var modelCredentialProviders = []struct {
	matches    func(model string) bool
	credential ServicePortalModelCredential
}{
	{
		matches: isGoogleModel,
		credential: ServicePortalModelCredential{
			Host:           "generativelanguage.googleapis.com",
			Protocol:       "https",
			Header:         "x-goog-api-key",
			SecretKey:      "GEMINI_API_KEY",
			CredentialName: "gemini-api-key",
		},
	},
	{
		matches: func(model string) bool {
			return strings.HasPrefix(strings.ToLower(strings.TrimSpace(model)), "openai/")
		},
		credential: ServicePortalModelCredential{
			Host:           "api.openai.com",
			Protocol:       "https",
			Header:         "Authorization",
			Prefix:         "Bearer ",
			SecretKey:      "OPENAI_API_KEY",
			CredentialName: "openai-api-key",
		},
	},
	{
		matches: func(model string) bool {
			return strings.HasPrefix(strings.ToLower(strings.TrimSpace(model)), "anthropic/")
		},
		credential: ServicePortalModelCredential{
			Host:           "api.anthropic.com",
			Protocol:       "https",
			Header:         "x-api-key",
			SecretKey:      "ANTHROPIC_API_KEY",
			CredentialName: "anthropic-api-key",
		},
	},
}

func isGoogleModel(model string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(model))
	return strings.HasPrefix(trimmed, "google/") || gemini.IsGeminiModel(model)
}

func parseServicePortalModelBaseURL(rawURL string) (protocol string, host string, port int32, err error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return "", "", 0, fmt.Errorf("invalid service portal model base URL %q: %w", rawURL, err)
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", "", 0, fmt.Errorf("invalid service portal model base URL %q: scheme must be http or https", rawURL)
	}
	if parsed.User != nil {
		return "", "", 0, fmt.Errorf("invalid service portal model base URL %q: userinfo is not allowed", rawURL)
	}
	hostname := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if hostname == "" {
		return "", "", 0, fmt.Errorf("invalid service portal model base URL %q: hostname is required", rawURL)
	}
	var parsedPort int32
	if portText := parsed.Port(); portText != "" {
		portNumber, parseErr := strconv.ParseUint(portText, 10, 16)
		if parseErr != nil || portNumber == 0 {
			return "", "", 0, fmt.Errorf("invalid service portal model base URL %q: invalid port %q", rawURL, portText)
		}
		parsedPort = int32(portNumber)
	}
	return scheme, hostname, parsedPort, nil
}

// ModelCredentialForModel returns the service portal credential mapping for a real model.
// Mock and unrecognized models return ok=false.
func ModelCredentialForModel(model string, modelBaseURL string) (ServicePortalModelCredential, bool, error) {
	if strings.TrimSpace(model) == "" || isMockModel(model) {
		return ServicePortalModelCredential{}, false, nil
	}
	for _, provider := range modelCredentialProviders {
		if provider.matches(model) {
			credential := provider.credential
			if strings.TrimSpace(modelBaseURL) != "" {
				protocol, host, port, err := parseServicePortalModelBaseURL(modelBaseURL)
				if err != nil {
					return ServicePortalModelCredential{}, false, err
				}
				credential.Protocol = protocol
				credential.Host = host
				credential.Port = port
			}
			return credential, true, nil
		}
	}
	return ServicePortalModelCredential{}, false, nil
}

// ClientOptions returns the serviceportalclient.Options corresponding to the service portal configuration.
func (options ServicePortalOptions) ClientOptions() serviceportalclient.Options {
	return serviceportalclient.Options{
		ControllerAddr: options.ControllerAddress,
		CAFile:         options.CAFile,
		TokenFile:      options.TokenFile,
	}
}

// Validate reports an error if the service portal configuration is invalid.
func (options ServicePortalOptions) Validate() error {
	if options.ControllerAddress != "" {
		if options.CAFile == "" {
			return errors.New("service portal CA file is required when controller address is set")
		}
		if options.TokenFile == "" {
			return errors.New("service portal token file is required when controller address is set")
		}
	}
	if strings.TrimSpace(options.ModelBaseURL) != "" {
		if _, _, _, err := parseServicePortalModelBaseURL(options.ModelBaseURL); err != nil {
			return err
		}
	}
	if trimmedNamespace := strings.TrimSpace(options.ServicePortalsNamespace); trimmedNamespace != "" {
		if errs := validation.IsDNS1123Label(trimmedNamespace); len(errs) > 0 {
			return fmt.Errorf("invalid service portals namespace %q: %s", options.ServicePortalsNamespace, strings.Join(errs, ", "))
		}
	}
	return nil
}

// BuildServicePortalEgressPolicy constructs an EgressPolicy proto for the given service portal options.
// The HTTP rule is populated with options.AllowedHTTPHosts; the HTTPS rule is populated with options.AllowedHosts.
func BuildServicePortalEgressPolicy(options ServicePortalOptions) *portalsv1.EgressPolicy {
	return BuildServicePortalEgressPolicyWithCredential(options, nil)
}

// BuildServicePortalEgressPolicyWithCredential constructs an EgressPolicy proto for the given service portal options
// and optional model credential rule replacing the provider's API key header from per-pod injected secrets.
func BuildServicePortalEgressPolicyWithCredential(
	options ServicePortalOptions,
	modelCredential *ServicePortalModelCredential,
) *portalsv1.EgressPolicy {
	return BuildServicePortalEgressPolicyWithCredentials(options, modelCredential, false)
}

// BuildServicePortalEgressPolicyWithCredentials constructs an EgressPolicy proto for the given service portal options,
// optional exact-host model credential rule, and optional *.googleapis.com GCP OAuth2 Bearer credential rule.
func BuildServicePortalEgressPolicyWithCredentials(
	options ServicePortalOptions,
	modelCredential *ServicePortalModelCredential,
	includeGCPAccessToken bool,
) *portalsv1.EgressPolicy {
	httpHosts := append([]string(nil), options.AllowedHTTPHosts...)
	httpsHosts := append([]string(nil), options.AllowedHosts...)

	var cachingServicePortalRules []*portalsv1.EgressRule
	for _, cachingServicePortal := range builtInCachingServicePortals {
		if !cachingServicePortal.enabled(options) {
			continue
		}
		for _, hostname := range cachingServicePortal.hostnames {
			normalizedHostname := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(hostname)), ".")
			httpsHosts = filterOutHostname(httpsHosts, normalizedHostname)
		}
		cachingServicePortalRules = append(cachingServicePortalRules, &portalsv1.EgressRule{
			Https: &portalsv1.HttpsRule{
				Hostnames: append([]string(nil), cachingServicePortal.hostnames...),
				Effects: &portalsv1.HttpRuleEffects{
					BackendUrl: cachingServicePortal.backendURL(options.ServicePortalsNamespace),
				},
			},
		})
	}

	var gcpCredentialRule *portalsv1.EgressRule
	if includeGCPAccessToken {
		httpsHosts = filterOutHostname(httpsHosts, gcpAccessTokenHost)
		gcpCredentialRule = &portalsv1.EgressRule{
			Https: &portalsv1.HttpsRule{
				Hostnames: []string{gcpAccessTokenHost},
				Effects: &portalsv1.HttpRuleEffects{
					ReplaceHeaders: []*portalsv1.CredentialHeader{
						{
							Header:        gcpAccessTokenHeader,
							Prefix:        gcpAccessTokenPrefix,
							CredentialUri: servicePortalCredentialURIScheme + gcpAccessTokenCredentialName,
						},
					},
				},
			},
		}
	}

	var credentialRule *portalsv1.EgressRule
	var credentialHost string
	var credentialEffectivePort int32
	if modelCredential != nil {
		credentialHost = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(modelCredential.Host)), ".")
		isHTTP := strings.EqualFold(modelCredential.Protocol, "http")
		effectivePort := modelCredential.Port
		if effectivePort == 0 {
			if isHTTP {
				effectivePort = 80
			} else {
				effectivePort = 443
			}
		}
		credentialEffectivePort = effectivePort
		// Remove exact duplicates on the same port from the plain allow lists so EgressPolicy.Validate() sees no ties.
		if effectivePort == 80 {
			httpHosts = filterOutHostname(httpHosts, credentialHost)
		}
		if effectivePort == 443 {
			httpsHosts = filterOutHostname(httpsHosts, credentialHost)
		}

		effects := &portalsv1.HttpRuleEffects{
			ReplaceHeaders: []*portalsv1.CredentialHeader{
				{
					Header:        modelCredential.Header,
					Prefix:        modelCredential.Prefix,
					CredentialUri: servicePortalCredentialURIScheme + modelCredential.CredentialName,
				},
			},
		}
		var ports *portalsv1.Ports
		if modelCredential.Port > 0 {
			ports = &portalsv1.Ports{Numbers: []int32{modelCredential.Port}}
		}
		if isHTTP {
			credentialRule = &portalsv1.EgressRule{
				Http: &portalsv1.HttpRule{
					Hostnames: []string{credentialHost},
					Ports:     ports,
					Effects:   effects,
				},
			}
		} else {
			credentialRule = &portalsv1.EgressRule{
				Https: &portalsv1.HttpsRule{
					Hostnames: []string{credentialHost},
					Ports:     ports,
					Effects:   effects,
				},
			}
		}
	}

	var registryServicePortalRule *portalsv1.EgressRule
	if options.RegistryServicePortalEnabled {
		registryHost := options.RegistryServicePortalHost()
		credentialMatchesRegistry := credentialRule != nil &&
			credentialHost == registryHost &&
			credentialEffectivePort == DefaultRegistryServicePortalPort
		if !credentialMatchesRegistry {
			registryServicePortalRule = &portalsv1.EgressRule{
				Http: &portalsv1.HttpRule{
					Hostnames: []string{registryHost},
					Ports: &portalsv1.Ports{
						Numbers: []int32{DefaultRegistryServicePortalPort},
					},
				},
			}
		}
	}

	if len(httpHosts) == 0 && len(httpsHosts) == 0 && len(cachingServicePortalRules) == 0 && registryServicePortalRule == nil && gcpCredentialRule == nil && credentialRule == nil {
		return &portalsv1.EgressPolicy{}
	}
	var rules []*portalsv1.EgressRule
	if len(httpHosts) > 0 {
		rules = append(rules, &portalsv1.EgressRule{
			Http: &portalsv1.HttpRule{
				Hostnames: httpHosts,
			},
		})
	}
	if len(httpsHosts) > 0 {
		rules = append(rules, &portalsv1.EgressRule{
			Https: &portalsv1.HttpsRule{
				Hostnames: httpsHosts,
			},
		})
	}
	rules = append(rules, cachingServicePortalRules...)
	if registryServicePortalRule != nil {
		rules = append(rules, registryServicePortalRule)
	}
	if gcpCredentialRule != nil {
		rules = append(rules, gcpCredentialRule)
	}
	if credentialRule != nil {
		rules = append(rules, credentialRule)
	}
	return &portalsv1.EgressPolicy{
		Rules: rules,
	}
}

func filterOutHostname(hosts []string, target string) []string {
	if len(hosts) == 0 {
		return nil
	}
	var filtered []string
	for _, host := range hosts {
		normalized := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
		if normalized == target {
			continue
		}
		filtered = append(filtered, host)
	}
	return filtered
}

func isValidHTTPHeaderValue(value string) bool {
	for i := 0; i < len(value); i++ {
		character := value[i]
		if character != '\t' && (character < ' ' || character == 0x7f) {
			return false
		}
	}
	return true
}

func redactSecrets(message string, secrets map[string][]byte) string {
	for _, secretBytes := range secrets {
		if len(secretBytes) == 0 {
			continue
		}
		message = strings.ReplaceAll(message, string(secretBytes), "[REDACTED]")
	}
	return message
}

func (r *RunReconciler) readControllerModelCredential(
	ctx context.Context,
	credential ServicePortalModelCredential,
) ([]byte, bool, error) {
	namespace := r.controllerNamespace()
	var secret corev1.Secret
	err := r.uncachedReader().Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      DefaultModelCredentialsSecretName,
	}, &secret)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("failed to read model credential secret %s/%s for host %q header %q: %w",
			namespace, DefaultModelCredentialsSecretName, credential.Host, credential.Header, err)
	}
	rawValue, ok := secret.Data[credential.SecretKey]
	if !ok {
		return nil, false, nil
	}
	trimmed := strings.TrimSpace(string(rawValue))
	if trimmed == "" {
		return nil, false, nil
	}
	if !isValidHTTPHeaderValue(trimmed) {
		return nil, false, fmt.Errorf("invalid model credential in secret %s/%s key %q for host %q header %q: value contains invalid HTTP header characters",
			namespace, DefaultModelCredentialsSecretName, credential.SecretKey, credential.Host, credential.Header)
	}
	return []byte(trimmed), true, nil
}

func (r *RunReconciler) mintGCPAccessToken(ctx context.Context, projectID string) ([]byte, error) {
	trimmedProjectID := strings.TrimSpace(projectID)
	if trimmedProjectID == "" {
		return nil, fmt.Errorf("failed to mint gcp access token for host %q header %q: task has no assigned gcp project", gcpAccessTokenHost, gcpAccessTokenHeader)
	}
	minter, err := r.gcpAccessTokenMinter(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create gcp access token minter for project %q host %q header %q: %s", trimmedProjectID, gcpAccessTokenHost, gcpAccessTokenHeader, redactGCPTokens(err.Error()))
	}
	accessToken, err := minter.MintAccessToken(ctx, trimmedProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to mint gcp access token for project %q host %q header %q: %s", trimmedProjectID, gcpAccessTokenHost, gcpAccessTokenHeader, redactGCPTokens(err.Error()))
	}
	trimmed := strings.TrimSpace(accessToken)
	if trimmed == "" {
		return nil, fmt.Errorf("failed to mint gcp access token for project %q host %q header %q: token is empty", trimmedProjectID, gcpAccessTokenHost, gcpAccessTokenHeader)
	}
	if !isValidHTTPHeaderValue(trimmed) {
		return nil, fmt.Errorf("invalid gcp access token for project %q host %q header %q: value contains invalid HTTP header characters", trimmedProjectID, gcpAccessTokenHost, gcpAccessTokenHeader)
	}
	return []byte(trimmed), nil
}

func (r *RunReconciler) prepareServicePortalBinding(
	ctx context.Context,
	run *v1alpha1.Run,
	manifest *v1alpha1.Manifest,
	taskID string,
	options ServicePortalOptions,
) (*portalsv1.EgressPolicy, map[string][]byte, *ServicePortalModelCredential, error) {
	manifestTask := findManifestTask(manifest, taskID)
	needsGCPAccessToken := manifestTask != nil && taskRequiresGCPProject(*manifestTask)

	var activeModelCredential *ServicePortalModelCredential
	var secrets map[string][]byte
	credential, hasCredential, err := ModelCredentialForModel(run.Spec.Model, options.ModelBaseURL)
	if err != nil {
		return nil, nil, nil, err
	}
	if hasCredential {
		secretValue, found, err := r.readControllerModelCredential(ctx, credential)
		if err != nil {
			return nil, nil, &credential, err
		}
		if found {
			activeModelCredential = &credential
			secrets = map[string][]byte{
				credential.CredentialName: secretValue,
			}
		}
	}

	if needsGCPAccessToken {
		projectID := strings.TrimSpace(taskGCPProject(run, taskID))
		if projectID == "" {
			return nil, nil, activeModelCredential, fmt.Errorf("failed to mint gcp access token for task %q: task has no assigned gcp project", taskID)
		}
		accessToken, err := r.mintGCPAccessToken(ctx, projectID)
		if err != nil {
			return nil, nil, activeModelCredential, err
		}
		if secrets == nil {
			secrets = make(map[string][]byte, 1)
		}
		secrets[gcpAccessTokenCredentialName] = accessToken
	}

	policy := BuildServicePortalEgressPolicyWithCredentials(options, activeModelCredential, needsGCPAccessToken)
	return policy, secrets, activeModelCredential, nil
}

func (r *RunReconciler) servicePortalPolicy() *portalsv1.EgressPolicy {
	r.cfgMu.RLock()
	servicePortal := r.Config.ServicePortal
	r.cfgMu.RUnlock()
	return BuildServicePortalEgressPolicy(servicePortal)
}

func (r *RunReconciler) servicePortalModelBaseURL() string {
	r.cfgMu.RLock()
	modelBaseURL := r.Config.ServicePortal.ModelBaseURL
	r.cfgMu.RUnlock()
	return modelBaseURL
}

func (r *RunReconciler) bindServicePortalPolicy(
	ctx context.Context,
	podUID string,
	policy *portalsv1.EgressPolicy,
	secrets map[string][]byte,
	modelCredential *ServicePortalModelCredential,
) error {
	if policy == nil {
		policy = r.servicePortalPolicy()
	}
	if err := r.ServicePortalClient.Bind(ctx, podUID, policy, secrets); err != nil {
		message := redactSecrets(err.Error(), secrets)
		if modelCredential != nil {
			return fmt.Errorf("service portal bind failed for host %q header %q: %s", modelCredential.Host, modelCredential.Header, message)
		}
		return fmt.Errorf("service portal bind failed: %s", message)
	}
	return nil
}

func (r *RunReconciler) isServicePortalPodBound(uid types.UID) bool {
	r.boundPodsMu.RLock()
	defer r.boundPodsMu.RUnlock()
	return r.boundServicePortalPods != nil && r.boundServicePortalPods[uid]
}

func (r *RunReconciler) markServicePortalPodBound(uid types.UID) {
	r.boundPodsMu.Lock()
	defer r.boundPodsMu.Unlock()
	if r.boundServicePortalPods == nil {
		r.boundServicePortalPods = make(map[types.UID]bool)
	}
	r.boundServicePortalPods[uid] = true
}

func (r *RunReconciler) unmarkServicePortalPodBound(uid types.UID) {
	r.boundPodsMu.Lock()
	defer r.boundPodsMu.Unlock()
	delete(r.boundServicePortalPods, uid)
}
