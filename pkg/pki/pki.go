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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strings"
	"time"
)

const (
	// DefaultCASecretName is the name of the Secret in the controller namespace storing the CA and controller client keypair.
	DefaultCASecretName = "sandbox-ca"

	// DefaultCAConfigMapName is the name of the ConfigMap in the sandbox namespace publishing the CA certificate.
	DefaultCAConfigMapName = "sandbox-ca"

	// Certificate and key keys in Secrets and ConfigMaps.
	CACertKey     = "ca.crt"
	CAKeyKey      = "ca.key"
	TLSCertKey    = "tls.crt"
	TLSKeyKey     = "tls.key"
	ClientCertKey = "client.crt"
	ClientKeyKey  = "client.key"

	// Default durations.
	DefaultCATTL         = 365 * 24 * time.Hour
	DefaultClientTTL     = 365 * 24 * time.Hour
	DefaultServerCertTTL = 24 * time.Hour
)

// ServerSAN constructs the DNS Subject Alternative Name that binds a server certificate to a specific sandbox.
func ServerSAN(sandboxName, sandboxNamespace string) string {
	return fmt.Sprintf("%s.%s.sandbox.evalrunner.local", sandboxName, sandboxNamespace)
}

// ServerSecretName returns a deterministic, DNS-1123 compliant Secret name for a sandbox's server certificate.
func ServerSecretName(sandboxName string) string {
	const prefix = "sb-tls-"
	const maxLen = 63
	full := prefix + sandboxName
	if len(full) <= maxLen {
		return full
	}
	hash := sha256.Sum256([]byte(sandboxName))
	suffix := "-" + hex.EncodeToString(hash[:4])
	avail := maxLen - len(prefix) - len(suffix)
	return prefix + strings.TrimRight(sandboxName[:avail], "-") + suffix
}

// CA represents a certificate authority capable of issuing server and client certificates.
type CA struct {
	caKey         *ecdsa.PrivateKey
	caCert        *x509.Certificate
	caCertPEM     []byte
	caPool        *x509.CertPool
	clientCert    tls.Certificate
	clientCertPEM []byte
	clientKeyPEM  []byte
}

// CACertPEM returns the PEM-encoded public CA certificate.
func (c *CA) CACertPEM() []byte {
	return c.caCertPEM
}

// ClientCertPEM returns the PEM-encoded client certificate.
func (c *CA) ClientCertPEM() []byte {
	return c.clientCertPEM
}

// ClientKeyPEM returns the PEM-encoded client private key.
func (c *CA) ClientKeyPEM() []byte {
	return c.clientKeyPEM
}

// CertPool returns a pool containing the CA certificate for verifying certificates issued by this CA.
func (c *CA) CertPool() *x509.CertPool {
	return c.caPool
}

// ClientTLSConfig returns a tls.Config configured for dialing a specific sandbox pod, pinning its expected SAN.
func (c *CA) ClientTLSConfig(sandboxName, sandboxNamespace string) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{c.clientCert},
		RootCAs:      c.caPool,
		ServerName:   ServerSAN(sandboxName, sandboxNamespace),
		MinVersion:   tls.VersionTLS12,
	}
}

// MintServerCert mints a short-lived server certificate and key signed by this CA for the given sandbox.
func (c *CA) MintServerCert(sandboxName, sandboxNamespace string, ttl time.Duration, ips ...net.IP) (certPEM, keyPEM []byte, err error) {
	if ttl <= 0 {
		ttl = DefaultServerCertTTL
	}

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate server private key: %w", err)
	}

	serialNumber, err := randSerialNumber()
	if err != nil {
		return nil, nil, err
	}

	dnsSAN := ServerSAN(sandboxName, sandboxNamespace)
	dnsNames := []string{
		dnsSAN,
		sandboxName,
		fmt.Sprintf("%s.%s.svc", sandboxName, sandboxNamespace),
	}

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   dnsSAN,
			Organization: []string{"eval-runner"},
		},
		NotBefore:             time.Now().Add(-5 * time.Minute),
		NotAfter:              time.Now().Add(ttl),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
		DNSNames:              dnsNames,
		IPAddresses:           ips,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, c.caCert, &serverKey.PublicKey, c.caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("create server certificate: %w", err)
	}

	certPEM = EncodeCertPEM(der)
	keyPEM, err = EncodeKeyPEM(serverKey)
	if err != nil {
		return nil, nil, err
	}
	return certPEM, keyPEM, nil
}

// NewInMemoryCA generates a fresh, self-contained CA and client certificate in memory.
func NewInMemoryCA() (*CA, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate CA key: %w", err)
	}

	caSerial, err := randSerialNumber()
	if err != nil {
		return nil, err
	}

	caTmpl := &x509.Certificate{
		SerialNumber: caSerial,
		Subject: pkix.Name{
			CommonName:   "eval-runner-ca",
			Organization: []string{"eval-runner"},
		},
		NotBefore:             time.Now().Add(-5 * time.Minute),
		NotAfter:              time.Now().Add(DefaultCATTL),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("create self-signed CA cert: %w", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, fmt.Errorf("parse generated CA cert: %w", err)
	}
	caCertPEM := EncodeCertPEM(caDER)

	// Client keypair
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate client key: %w", err)
	}
	clientSerial, err := randSerialNumber()
	if err != nil {
		return nil, err
	}
	clientTmpl := &x509.Certificate{
		SerialNumber: clientSerial,
		Subject: pkix.Name{
			CommonName:   "eval-runner-controller",
			Organization: []string{"eval-runner"},
		},
		NotBefore:             time.Now().Add(-5 * time.Minute),
		NotAfter:              time.Now().Add(DefaultClientTTL),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
		DNSNames:              []string{"eval-runner-controller", "eval-runner-controller.eval-runner.svc"},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTmpl, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("create client cert: %w", err)
	}
	clientCertPEM := EncodeCertPEM(clientDER)
	clientKeyPEM, err := EncodeKeyPEM(clientKey)
	if err != nil {
		return nil, err
	}

	clientTLS, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("load client key pair: %w", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	return &CA{
		caKey:         caKey,
		caCert:        caCert,
		caCertPEM:     caCertPEM,
		caPool:        pool,
		clientCert:    clientTLS,
		clientCertPEM: clientCertPEM,
		clientKeyPEM:  clientKeyPEM,
	}, nil
}

// LoadCA loads a CA from PEM-encoded CA cert/key and client cert/key.
func LoadCA(caCertPEM, caKeyPEM, clientCertPEM, clientKeyPEM []byte) (*CA, error) {
	caCert, err := DecodeCertPEM(caCertPEM)
	if err != nil {
		return nil, fmt.Errorf("decode CA cert: %w", err)
	}
	caKey, err := DecodeKeyPEM(caKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("decode CA key: %w", err)
	}
	clientTLS, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("load client key pair: %w", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	return &CA{
		caKey:         caKey,
		caCert:        caCert,
		caCertPEM:     caCertPEM,
		caPool:        pool,
		clientCert:    clientTLS,
		clientCertPEM: clientCertPEM,
		clientKeyPEM:  clientKeyPEM,
	}, nil
}

// EncodeCertPEM encodes DER bytes into a PEM certificate block.
func EncodeCertPEM(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: der,
	})
}

// EncodeKeyPEM encodes an ECDSA private key into a PEM block.
func EncodeKeyPEM(key *ecdsa.PrivateKey) ([]byte, error) {
	bytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("marshal EC private key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: bytes,
	}), nil
}

// DecodeCertPEM parses the first PEM certificate block from pemBytes.
func DecodeCertPEM(pemBytes []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errors.New("no valid CERTIFICATE PEM block found")
	}
	return x509.ParseCertificate(block.Bytes)
}

// DecodeKeyPEM parses an EC or PKCS8 private key from PEM bytes.
func DecodeKeyPEM(pemBytes []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("no PEM block found")
	}
	if block.Type == "EC PRIVATE KEY" {
		return x509.ParseECPrivateKey(block.Bytes)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse PKCS8 private key: %w", err)
	}
	ecKey, ok := key.(*ecdsa.PrivateKey)
	if !ok {
		return nil, errors.New("key is not an ECDSA private key")
	}
	return ecKey, nil
}

func randSerialNumber() (*big.Int, error) {
	max := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, max)
	if err != nil {
		return nil, fmt.Errorf("generate random serial number: %w", err)
	}
	return serial, nil
}
