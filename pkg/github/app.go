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

package github

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ParseRSAPrivateKey parses an RSA private key in PKCS#1 or PKCS#8 PEM format.
func ParseRSAPrivateKey(pemBytes []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("failed to decode PEM block from private key")
	}

	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err == nil {
		return key, nil
	}

	parsedKey, err2 := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err2 == nil {
		if rsaKey, ok := parsedKey.(*rsa.PrivateKey); ok {
			return rsaKey, nil
		}
		return nil, errors.New("PKCS#8 key is not an RSA private key")
	}

	return nil, fmt.Errorf("could not parse private key as PKCS#1 (%v) or PKCS#8 (%v)", err, err2)
}

// GenerateAppJWT generates a signed RS256 JWT for GitHub App authentication.
func GenerateAppJWT(appID string, key *rsa.PrivateKey, now time.Time) (string, error) {
	if key == nil {
		return "", errors.New("private key is nil")
	}
	if strings.TrimSpace(appID) == "" {
		return "", errors.New("appID is empty")
	}

	headerJSON := `{"alg":"RS256","typ":"JWT"}`
	claimsJSON := fmt.Sprintf(`{"iat":%d,"exp":%d,"iss":%s}`,
		now.Add(-60*time.Second).Unix(),
		now.Add(9*time.Minute).Unix(),
		jsonString(appID),
	)

	signingInput := base64.RawURLEncoding.EncodeToString([]byte(headerJSON)) + "." +
		base64.RawURLEncoding.EncodeToString([]byte(claimsJSON))

	hashed := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, hashed[:])
	if err != nil {
		return "", fmt.Errorf("failed to sign JWT: %w", err)
	}

	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func jsonString(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		return `""`
	}
	return string(b)
}

// AppConfig holds parameters to construct an AppTokenProvider.
type AppConfig struct {
	AppID          string
	InstallationID string
	PrivateKeyPEM  []byte
	AppSlug        string
	BaseURL        string
	HTTPClient     *http.Client
}

// AppTokenProvider handles GitHub App authentication: generates JWTs from the App ID
// and private key, exchanges them for installation access tokens, and caches the tokens.
type AppTokenProvider struct {
	appID          string
	installationID string
	privateKey     *rsa.PrivateKey
	appSlug        string
	baseURL        string
	httpClient     *http.Client
	nowFunc        func() time.Time

	mu           sync.Mutex
	cachedToken  string
	expiresAt    time.Time
	resolvedSlug string
}

// NewAppTokenProvider creates a new AppTokenProvider.
func NewAppTokenProvider(cfg AppConfig) (*AppTokenProvider, error) {
	appID := strings.TrimSpace(cfg.AppID)
	if appID == "" {
		return nil, errors.New("missing AppID")
	}
	instID := strings.TrimSpace(cfg.InstallationID)
	if instID == "" {
		return nil, errors.New("missing InstallationID")
	}
	key, err := ParseRSAPrivateKey(cfg.PrivateKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("invalid private key: %w", err)
	}

	baseURL := strings.TrimRight(cfg.BaseURL, "/")
	if baseURL == "" {
		baseURL = "https://api.github.com"
	}

	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}

	return &AppTokenProvider{
		appID:          appID,
		installationID: instID,
		privateKey:     key,
		appSlug:        strings.TrimSpace(cfg.AppSlug),
		baseURL:        baseURL,
		httpClient:     hc,
		nowFunc:        time.Now,
	}, nil
}

type installationTokenResponse struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Token returns a cached installation access token, refreshing it if expired or near expiry (within 5 minutes).
func (a *AppTokenProvider) Token(ctx context.Context) (_ string, retErr error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := a.nowFunc()
	if a.cachedToken != "" && now.Add(5*time.Minute).Before(a.expiresAt) {
		return a.cachedToken, nil
	}

	jwt, err := GenerateAppJWT(a.appID, a.privateKey, now)
	if err != nil {
		return "", fmt.Errorf("failed to generate app JWT: %w", err)
	}

	url := fmt.Sprintf("%s/app/installations/%s/access_tokens", a.baseURL, a.installationID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader([]byte("{}")))
	if err != nil {
		return "", fmt.Errorf("failed to create access token request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", &ServerError{StatusCode: 0, Message: fmt.Sprintf("failed to request installation token: %v", err)}
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			if retErr == nil {
				retErr = fmt.Errorf("close response body: %w", closeErr)
			}
		}
	}()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read access token response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		cleanBody := strings.TrimSpace(string(bodyBytes))
		if len(cleanBody) > 256 {
			cleanBody = cleanBody[:256] + "..."
		}
		if resp.StatusCode >= 500 {
			return "", &ServerError{StatusCode: resp.StatusCode, Message: fmt.Sprintf("github server error (%d): %s", resp.StatusCode, cleanBody)}
		}
		return "", &PermanentError{StatusCode: resp.StatusCode, Message: fmt.Sprintf("failed to get installation token (%d): %s", resp.StatusCode, cleanBody)}
	}

	var tokenResp installationTokenResponse
	if err := json.Unmarshal(bodyBytes, &tokenResp); err != nil {
		return "", fmt.Errorf("failed to parse installation token response: %w", err)
	}

	a.cachedToken = tokenResp.Token
	a.expiresAt = tokenResp.ExpiresAt
	return a.cachedToken, nil
}

type appInfoResponse struct {
	ID   int64  `json:"id"`
	Slug string `json:"slug"`
}

// Identity returns the GitHub App's bot user login in "<slug>[bot]" format.
func (a *AppTokenProvider) Identity(ctx context.Context) (_ string, retErr error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.resolvedSlug != "" {
		return a.resolvedSlug + "[bot]", nil
	}
	if a.appSlug != "" {
		a.resolvedSlug = a.appSlug
		return a.resolvedSlug + "[bot]", nil
	}

	now := a.nowFunc()
	jwt, err := GenerateAppJWT(a.appID, a.privateKey, now)
	if err != nil {
		return "", &PermanentError{StatusCode: http.StatusBadRequest, Message: fmt.Sprintf("failed to generate app JWT: %v", err)}
	}

	url := fmt.Sprintf("%s/app", a.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create get app request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", &ServerError{StatusCode: 0, Message: fmt.Sprintf("failed to get app info: %v", err)}
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			if retErr == nil {
				retErr = fmt.Errorf("close response body: %w", closeErr)
			}
		}
	}()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read app response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		cleanBody := strings.TrimSpace(string(bodyBytes))
		if len(cleanBody) > 256 {
			cleanBody = cleanBody[:256] + "..."
		}
		if resp.StatusCode >= 500 {
			return "", &ServerError{StatusCode: resp.StatusCode, Message: fmt.Sprintf("github server error (%d): %s", resp.StatusCode, cleanBody)}
		}
		return "", &PermanentError{StatusCode: resp.StatusCode, Message: fmt.Sprintf("failed to get app info (%d): %s", resp.StatusCode, cleanBody)}
	}

	var appResp appInfoResponse
	if err := json.Unmarshal(bodyBytes, &appResp); err != nil {
		return "", &PermanentError{StatusCode: resp.StatusCode, Message: fmt.Sprintf("failed to parse app info response: %v", err)}
	}

	if appResp.Slug == "" {
		return "", &PermanentError{StatusCode: http.StatusUnprocessableEntity, Message: "github /app response did not contain an app slug"}
	}
	a.resolvedSlug = appResp.Slug

	return a.resolvedSlug + "[bot]", nil
}
