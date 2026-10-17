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

package pki

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Manager coordinates CA bootstrapping, ConfigMap publication in the sandbox namespace,
// and issuance of per-sandbox server certificate Secrets.
type Manager struct {
	client              client.Client
	controllerNamespace string
	sandboxNamespace    string

	mu sync.RWMutex
	ca *CA
}

// NewManager creates a new PKI Manager.
func NewManager(c client.Client, controllerNamespace, sandboxNamespace string) *Manager {
	if controllerNamespace == "" {
		controllerNamespace = "eval-runner"
	}
	if sandboxNamespace == "" {
		sandboxNamespace = "eval-runner-sandbox"
	}
	return &Manager{
		client:              c,
		controllerNamespace: controllerNamespace,
		sandboxNamespace:    sandboxNamespace,
	}
}

// EnsureCA idempotently bootstraps the CA Secret in the controller namespace and the
// public CA ConfigMap in the sandbox namespace. It is safe to call across controller restarts.
func (m *Manager) EnsureCA(ctx context.Context) (*CA, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ca != nil {
		return m.ca, nil
	}

	secretKey := types.NamespacedName{
		Namespace: m.controllerNamespace,
		Name:      DefaultCASecretName,
	}

	var caSecret corev1.Secret
	err := m.client.Get(ctx, secretKey, &caSecret)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("read CA secret %s/%s: %w", m.controllerNamespace, DefaultCASecretName, err)
	}

	var ca *CA
	if apierrors.IsNotFound(err) {
		// Absent: generate new CA + client keypair and persist in Secret
		newCA, err := NewInMemoryCA()
		if err != nil {
			return nil, fmt.Errorf("generate CA: %w", err)
		}

		caKeyPEM, err := EncodeKeyPEM(newCA.caKey)
		if err != nil {
			return nil, err
		}

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      DefaultCASecretName,
				Namespace: m.controllerNamespace,
			},
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{
				CACertKey:     newCA.caCertPEM,
				CAKeyKey:      caKeyPEM,
				ClientCertKey: newCA.clientCertPEM,
				ClientKeyKey:  newCA.clientKeyPEM,
			},
		}

		if err := m.client.Create(ctx, secret); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return nil, fmt.Errorf("create CA secret %s/%s: %w", m.controllerNamespace, DefaultCASecretName, err)
			}
			// Raced with another replica: re-fetch
			if err := m.client.Get(ctx, secretKey, &caSecret); err != nil {
				return nil, fmt.Errorf("re-read CA secret: %w", err)
			}
			loadedCA, err := loadCAFromSecret(&caSecret)
			if err != nil {
				return nil, err
			}
			ca = loadedCA
		} else {
			ca = newCA
		}
	} else {
		// Existing Secret: load CA + client keypair
		loadedCA, err := loadCAFromSecret(&caSecret)
		if err != nil {
			return nil, err
		}
		ca = loadedCA
	}

	// Publish public CA cert into ConfigMap in sandbox namespace
	cmKey := types.NamespacedName{
		Namespace: m.sandboxNamespace,
		Name:      DefaultCAConfigMapName,
	}
	var caCM corev1.ConfigMap
	cmErr := m.client.Get(ctx, cmKey, &caCM)
	if apierrors.IsNotFound(cmErr) {
		desiredCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      DefaultCAConfigMapName,
				Namespace: m.sandboxNamespace,
			},
			Data: map[string]string{
				CACertKey: string(ca.caCertPEM),
			},
		}
		if err := m.client.Create(ctx, desiredCM); err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("create CA configmap %s/%s: %w", m.sandboxNamespace, DefaultCAConfigMapName, err)
		}
	} else if cmErr == nil {
		if caCM.Data == nil || caCM.Data[CACertKey] != string(ca.caCertPEM) {
			if caCM.Data == nil {
				caCM.Data = make(map[string]string)
			}
			caCM.Data[CACertKey] = string(ca.caCertPEM)
			if err := m.client.Update(ctx, &caCM); err != nil {
				return nil, fmt.Errorf("update CA configmap %s/%s: %w", m.sandboxNamespace, DefaultCAConfigMapName, err)
			}
		}
	} else {
		return nil, fmt.Errorf("read CA configmap %s/%s: %w", m.sandboxNamespace, DefaultCAConfigMapName, cmErr)
	}

	m.ca = ca
	return ca, nil
}

// CA returns the active CA if bootstrapped, or nil.
func (m *Manager) CA() *CA {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ca
}

// SetCA sets the active CA (useful for testing or direct injection).
func (m *Manager) SetCA(ca *CA) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ca = ca
}

// ClientTLSConfig returns a tls.Config configured for dialing the specified sandbox.
func (m *Manager) ClientTLSConfig(sandboxName, sandboxNamespace string) *tls.Config {
	ca := m.CA()
	if ca == nil {
		return nil
	}
	return ca.ClientTLSConfig(sandboxName, sandboxNamespace)
}

// EnsureServerCertSecret mints a server certificate for sandboxName and creates the
// corresponding Secret in the sandbox namespace.
func (m *Manager) EnsureServerCertSecret(ctx context.Context, sandboxName string, ttl time.Duration) (*corev1.Secret, error) {
	ca, err := m.EnsureCA(ctx)
	if err != nil {
		return nil, fmt.Errorf("ensure CA before minting cert: %w", err)
	}

	secretName := ServerSecretName(sandboxName)
	certPEM, keyPEM, err := ca.MintServerCert(sandboxName, m.sandboxNamespace, ttl)
	if err != nil {
		return nil, fmt.Errorf("mint server cert for %s: %w", sandboxName, err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: m.sandboxNamespace,
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			TLSCertKey: certPEM,
			TLSKeyKey:  keyPEM,
		},
	}

	if err := m.client.Create(ctx, secret); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return secret, nil
		}
		return nil, fmt.Errorf("create server cert secret %s: %w", secretName, err)
	}

	return secret, nil
}

// DeleteServerCertSecret explicitly removes the server certificate Secret for a sandbox.
func (m *Manager) DeleteServerCertSecret(ctx context.Context, sandboxName string) error {
	secretName := ServerSecretName(sandboxName)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: m.sandboxNamespace,
		},
	}
	if err := m.client.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete server cert secret %s: %w", secretName, err)
	}
	return nil
}

func loadCAFromSecret(secret *corev1.Secret) (*CA, error) {
	caCertPEM := secret.Data[CACertKey]
	if len(caCertPEM) == 0 {
		caCertPEM = secret.Data[TLSCertKey]
	}
	caKeyPEM := secret.Data[CAKeyKey]
	if len(caKeyPEM) == 0 {
		caKeyPEM = secret.Data[TLSKeyKey]
	}
	clientCertPEM := secret.Data[ClientCertKey]
	clientKeyPEM := secret.Data[ClientKeyKey]

	if len(caCertPEM) == 0 || len(caKeyPEM) == 0 {
		return nil, fmt.Errorf("CA secret %s/%s missing ca.crt or ca.key", secret.Namespace, secret.Name)
	}
	if len(clientCertPEM) == 0 || len(clientKeyPEM) == 0 {
		return nil, fmt.Errorf("CA secret %s/%s missing client.crt or client.key", secret.Namespace, secret.Name)
	}

	return LoadCA(caCertPEM, caKeyPEM, clientCertPEM, clientKeyPEM)
}
