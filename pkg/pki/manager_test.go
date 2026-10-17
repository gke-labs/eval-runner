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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestManager_EnsureCA_Idempotent(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	ctx := t.Context()

	mgr := NewManager(client, "eval-runner", "eval-runner-sandbox")

	// 1. Initial bootstrap
	ca1, err := mgr.EnsureCA(ctx)
	if err != nil {
		t.Fatalf("First EnsureCA failed: %v", err)
	}

	// Verify CA secret created in eval-runner
	var sec corev1.Secret
	if err := client.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: DefaultCASecretName}, &sec); err != nil {
		t.Fatalf("CA secret not found in eval-runner: %v", err)
	}
	if len(sec.Data[CACertKey]) == 0 || len(sec.Data[CAKeyKey]) == 0 {
		t.Errorf("CA secret missing cert or key")
	}

	// Verify CA ConfigMap created in eval-runner-sandbox
	var cm corev1.ConfigMap
	if err := client.Get(ctx, types.NamespacedName{Namespace: "eval-runner-sandbox", Name: DefaultCAConfigMapName}, &cm); err != nil {
		t.Fatalf("CA configmap not found in eval-runner-sandbox: %v", err)
	}
	if cm.Data[CACertKey] != string(ca1.CACertPEM()) {
		t.Errorf("ConfigMap ca.crt does not match CA cert PEM")
	}

	// 2. Restart simulation: new manager instance pointing at same cluster
	mgr2 := NewManager(client, "eval-runner", "eval-runner-sandbox")
	ca2, err := mgr2.EnsureCA(ctx)
	if err != nil {
		t.Fatalf("Second EnsureCA failed: %v", err)
	}

	if string(ca2.CACertPEM()) != string(ca1.CACertPEM()) {
		t.Errorf("CA certificate differed across restart simulation")
	}
}

func TestManager_EnsureServerCertSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	ctx := t.Context()

	mgr := NewManager(client, "eval-runner", "eval-runner-sandbox")

	secret, err := mgr.EnsureServerCertSecret(ctx, "sandbox-123", 2*time.Hour)
	if err != nil {
		t.Fatalf("EnsureServerCertSecret failed: %v", err)
	}

	if secret.Namespace != "eval-runner-sandbox" {
		t.Errorf("expected namespace 'eval-runner-sandbox', got %q", secret.Namespace)
	}
	expectedName := ServerSecretName("sandbox-123")
	if secret.Name != expectedName {
		t.Errorf("expected secret name %q, got %q", expectedName, secret.Name)
	}
	if len(secret.Data[TLSCertKey]) == 0 || len(secret.Data[TLSKeyKey]) == 0 {
		t.Errorf("expected secret to contain tls.crt and tls.key")
	}

	// Re-calling is idempotent and returns existing
	sec2, err := mgr.EnsureServerCertSecret(ctx, "sandbox-123", 2*time.Hour)
	if err != nil {
		t.Fatalf("Second EnsureServerCertSecret failed: %v", err)
	}
	if sec2.Name != secret.Name {
		t.Errorf("expected same secret, got: %v", sec2.Name)
	}

	// Delete
	if err := mgr.DeleteServerCertSecret(ctx, "sandbox-123"); err != nil {
		t.Fatalf("DeleteServerCertSecret failed: %v", err)
	}

	var deleted corev1.Secret
	err = client.Get(ctx, types.NamespacedName{Namespace: "eval-runner-sandbox", Name: expectedName}, &deleted)
	if err == nil {
		t.Errorf("expected secret to be deleted, but it was found")
	}
}
