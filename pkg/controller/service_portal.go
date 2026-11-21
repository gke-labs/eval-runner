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

	serviceportalclient "github.com/gke-labs/service-portals/pkg/client"
	portalsv1 "github.com/gke-labs/service-portals/pkg/portals/v1"
	"k8s.io/apimachinery/pkg/types"
)

// DefaultServicePortalAllowedHosts defines the default allowed hostnames for task sandboxes behind the service portal:
// - Model APIs (*.googleapis.com)
// - Git hosts (github.com, *.github.com, *.githubusercontent.com)
// - Python package repositories (pypi.org, *.pypi.org, files.pythonhosted.org, *.pythonhosted.org)
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
	return nil
}

// BuildServicePortalEgressPolicy constructs an EgressPolicy proto for the given service portal options.
// The HTTP rule is populated with options.AllowedHTTPHosts; the HTTPS rule is populated with options.AllowedHosts.
func BuildServicePortalEgressPolicy(options ServicePortalOptions) *portalsv1.EgressPolicy {
	if len(options.AllowedHosts) == 0 && len(options.AllowedHTTPHosts) == 0 {
		return &portalsv1.EgressPolicy{}
	}
	var rules []*portalsv1.EgressRule
	if len(options.AllowedHTTPHosts) > 0 {
		rules = append(rules, &portalsv1.EgressRule{
			Http: &portalsv1.HttpRule{
				Hostnames: append([]string(nil), options.AllowedHTTPHosts...),
			},
		})
	}
	if len(options.AllowedHosts) > 0 {
		rules = append(rules, &portalsv1.EgressRule{
			Https: &portalsv1.HttpsRule{
				Hostnames: append([]string(nil), options.AllowedHosts...),
			},
		})
	}
	return &portalsv1.EgressPolicy{
		Rules: rules,
	}
}

func (r *RunReconciler) servicePortalPolicy() *portalsv1.EgressPolicy {
	r.cfgMu.RLock()
	servicePortal := r.Config.ServicePortal
	r.cfgMu.RUnlock()
	return BuildServicePortalEgressPolicy(servicePortal)
}

func (r *RunReconciler) bindServicePortalPolicy(ctx context.Context, podUID string, policy *portalsv1.EgressPolicy) error {
	if policy == nil {
		policy = r.servicePortalPolicy()
	}
	if err := r.ServicePortalClient.Bind(ctx, podUID, policy, nil); err != nil {
		return fmt.Errorf("service portal bind failed: %w", err)
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
