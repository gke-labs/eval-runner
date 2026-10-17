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
	"crypto/tls"
	"crypto/x509"
	"net"
	"testing"
	"time"
)

func TestNewInMemoryCA(t *testing.T) {
	ca, err := NewInMemoryCA()
	if err != nil {
		t.Fatalf("NewInMemoryCA failed: %v", err)
	}

	if len(ca.CACertPEM()) == 0 {
		t.Errorf("expected non-empty CA cert PEM")
	}
	if len(ca.ClientCertPEM()) == 0 {
		t.Errorf("expected non-empty client cert PEM")
	}
	if len(ca.ClientKeyPEM()) == 0 {
		t.Errorf("expected non-empty client key PEM")
	}
	if ca.CertPool() == nil {
		t.Errorf("expected non-nil CertPool")
	}

	// Verify client certificate chains to CA
	clientCert, err := DecodeCertPEM(ca.ClientCertPEM())
	if err != nil {
		t.Fatalf("Decode client cert failed: %v", err)
	}
	opts := x509.VerifyOptions{
		Roots:     ca.CertPool(),
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	if _, err := clientCert.Verify(opts); err != nil {
		t.Errorf("client cert failed verification against CA: %v", err)
	}
}

func TestMintServerCertAndSANPinning(t *testing.T) {
	ca, err := NewInMemoryCA()
	if err != nil {
		t.Fatalf("NewInMemoryCA failed: %v", err)
	}

	sandboxName := "discovery-run-123"
	sandboxNamespace := "eval-runner-sandbox"
	ip := net.ParseIP("10.244.0.50")

	certPEM, keyPEM, err := ca.MintServerCert(sandboxName, sandboxNamespace, 1*time.Hour, ip)
	if err != nil {
		t.Fatalf("MintServerCert failed: %v", err)
	}
	if len(certPEM) == 0 || len(keyPEM) == 0 {
		t.Fatalf("expected non-empty cert and key PEM")
	}

	cert, err := DecodeCertPEM(certPEM)
	if err != nil {
		t.Fatalf("DecodeCertPEM failed: %v", err)
	}

	expectedSAN := ServerSAN(sandboxName, sandboxNamespace)
	if cert.Subject.CommonName != expectedSAN {
		t.Errorf("expected CommonName %q, got %q", expectedSAN, cert.Subject.CommonName)
	}

	// Verify server cert chains to CA
	opts := x509.VerifyOptions{
		Roots:     ca.CertPool(),
		DNSName:   expectedSAN,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if _, err := cert.Verify(opts); err != nil {
		t.Errorf("server cert failed verification for expected SAN: %v", err)
	}

	// Verify wrong SAN is rejected
	wrongOpts := x509.VerifyOptions{
		Roots:     ca.CertPool(),
		DNSName:   "other-sandbox.eval-runner-sandbox.sandbox.evalrunner.local",
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if _, err := cert.Verify(wrongOpts); err == nil {
		t.Errorf("expected server cert verification to fail for mismatched SAN, but it succeeded")
	}
}

func TestServerSecretName(t *testing.T) {
	shortName := "discovery-abc"
	sName := ServerSecretName(shortName)
	if sName != "sb-tls-discovery-abc" {
		t.Errorf("expected 'sb-tls-discovery-abc', got %q", sName)
	}

	longName := "task-extremely-long-task-identifier-that-exceeds-kubernetes-sixty-three-character-limit"
	longSName := ServerSecretName(longName)
	if len(longSName) > 63 {
		t.Errorf("expected ServerSecretName <= 63 characters, got %d (%q)", len(longSName), longSName)
	}
}

func TestClientTLSConfig(t *testing.T) {
	ca, err := NewInMemoryCA()
	if err != nil {
		t.Fatalf("NewInMemoryCA failed: %v", err)
	}

	cfg := ca.ClientTLSConfig("my-sandbox", "eval-runner-sandbox")
	if cfg == nil {
		t.Fatalf("expected non-nil tls.Config")
	}
	if cfg.ServerName != "my-sandbox.eval-runner-sandbox.sandbox.evalrunner.local" {
		t.Errorf("expected ServerName %q, got %q", "my-sandbox.eval-runner-sandbox.sandbox.evalrunner.local", cfg.ServerName)
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("expected MinVersion TLS 1.2, got %d", cfg.MinVersion)
	}
	if len(cfg.Certificates) != 1 {
		t.Errorf("expected 1 client certificate, got %d", len(cfg.Certificates))
	}
}

func TestLoadCA_RoundTrip(t *testing.T) {
	orig, err := NewInMemoryCA()
	if err != nil {
		t.Fatalf("NewInMemoryCA failed: %v", err)
	}

	keyPEM, err := EncodeKeyPEM(orig.caKey)
	if err != nil {
		t.Fatalf("EncodeKeyPEM failed: %v", err)
	}

	loaded, err := LoadCA(orig.CACertPEM(), keyPEM, orig.ClientCertPEM(), orig.ClientKeyPEM())
	if err != nil {
		t.Fatalf("LoadCA failed: %v", err)
	}

	if loaded.caCert.Subject.CommonName != orig.caCert.Subject.CommonName {
		t.Errorf("expected CommonName %q, got %q", orig.caCert.Subject.CommonName, loaded.caCert.Subject.CommonName)
	}
}
