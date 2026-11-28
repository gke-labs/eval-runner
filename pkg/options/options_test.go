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

package options_test

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/pkg/options"
)

// modeValue implements encoding.TextUnmarshaler and fmt.Stringer (without TextMarshaler).
type modeValue struct {
	value string
}

func (mode *modeValue) UnmarshalText(text []byte) error {
	normalized := strings.ToLower(strings.TrimSpace(string(text)))
	switch normalized {
	case "order", "prune":
		mode.value = normalized
		return nil
	default:
		return fmt.Errorf("unsupported mode %q", string(text))
	}
}

func (mode modeValue) String() string {
	return mode.value
}

// opaqueToken implements ONLY encoding.TextUnmarshaler (neither TextMarshaler nor fmt.Stringer).
type opaqueToken struct {
	secret string
}

func (token *opaqueToken) UnmarshalText(text []byte) error {
	trimmed := strings.TrimSpace(string(text))
	if trimmed == "bad" {
		return errors.New("rejected token")
	}
	token.secret = trimmed
	return nil
}

func TestAcronymNamingAllFormats(t *testing.T) {
	type NamingOptions struct {
		Bar                   string
		FooBar                int
		ModelBaseURL          string
		GCPProjectID          string
		AllowedHTTPHosts      []string
		UIBaseURL             string
		CAFile                string
		ComparisonTTL         time.Duration
		DefaultBudgetUSD      float64
		HeadTargetCIHalfWidth float64
		URL                   string
		ID                    string
		S3Bucket              string
		HTTP2Enabled          bool
	}

	expected := []struct {
		flagName string
		envName  string
		mapKey   string
		value    string
	}{
		{flagName: "bar", envName: "EVALRUNNER_BAR", mapKey: "bar", value: "alpha"},
		{flagName: "foo-bar", envName: "EVALRUNNER_FOO_BAR", mapKey: "fooBar", value: "42"},
		{flagName: "model-base-url", envName: "EVALRUNNER_MODEL_BASE_URL", mapKey: "modelBaseURL", value: "https://model.example"},
		{flagName: "gcp-project-id", envName: "EVALRUNNER_GCP_PROJECT_ID", mapKey: "gcpProjectID", value: "my-project"},
		{flagName: "allowed-http-hosts", envName: "EVALRUNNER_ALLOWED_HTTP_HOSTS", mapKey: "allowedHTTPHosts", value: "a.example,b.example"},
		{flagName: "ui-base-url", envName: "EVALRUNNER_UI_BASE_URL", mapKey: "uiBaseURL", value: "https://ui.example"},
		{flagName: "ca-file", envName: "EVALRUNNER_CA_FILE", mapKey: "caFile", value: "/etc/ca.pem"},
		{flagName: "comparison-ttl", envName: "EVALRUNNER_COMPARISON_TTL", mapKey: "comparisonTTL", value: "24h"},
		{flagName: "default-budget-usd", envName: "EVALRUNNER_DEFAULT_BUDGET_USD", mapKey: "defaultBudgetUSD", value: "12.5"},
		{flagName: "head-target-ci-half-width", envName: "EVALRUNNER_HEAD_TARGET_CI_HALF_WIDTH", mapKey: "headTargetCIHalfWidth", value: "0.05"},
		{flagName: "url", envName: "EVALRUNNER_URL", mapKey: "url", value: "https://example.com"},
		{flagName: "id", envName: "EVALRUNNER_ID", mapKey: "id", value: "abc-123"},
		{flagName: "s3-bucket", envName: "EVALRUNNER_S3_BUCKET", mapKey: "s3Bucket", value: "bucket-1"},
		{flagName: "http2-enabled", envName: "EVALRUNNER_HTTP2_ENABLED", mapKey: "http2Enabled", value: "true"},
	}

	// Verify flags
	var flagTarget NamingOptions
	flagSet := flag.NewFlagSet("naming", flag.ContinueOnError)
	if err := options.BindFlags(flagSet, &flagTarget); err != nil {
		t.Fatalf("BindFlags failed: %v", err)
	}
	var flagArguments []string
	for _, item := range expected {
		if flagSet.Lookup(item.flagName) == nil {
			t.Errorf("expected flag --%s to be registered", item.flagName)
		}
		flagArguments = append(flagArguments, fmt.Sprintf("--%s=%s", item.flagName, item.value))
	}
	if err := flagSet.Parse(flagArguments); err != nil {
		t.Fatalf("flagSet.Parse failed: %v", err)
	}

	// Verify env vars
	envMap := make(map[string]string, len(expected))
	for _, item := range expected {
		envMap[item.envName] = item.value
	}
	var envTarget NamingOptions
	if err := options.LoadEnvFrom(&envTarget, options.DefaultEnvPrefix, func(key string) (string, bool) {
		value, ok := envMap[key]
		return value, ok
	}); err != nil {
		t.Fatalf("LoadEnvFrom failed: %v", err)
	}

	// Verify map keys
	mapData := make(map[string]string, len(expected))
	for _, item := range expected {
		mapData[item.mapKey] = item.value
	}
	var mapTarget NamingOptions
	if err := options.LoadMap(&mapTarget, mapData); err != nil {
		t.Fatalf("LoadMap failed: %v", err)
	}
	unknownKeys, err := options.UnknownMapKeys(&mapTarget, mapData)
	if err != nil {
		t.Fatalf("UnknownMapKeys failed: %v", err)
	}
	if len(unknownKeys) != 0 {
		t.Errorf("expected no unknown map keys, got %v", unknownKeys)
	}

	want := NamingOptions{
		Bar:                   "alpha",
		FooBar:                42,
		ModelBaseURL:          "https://model.example",
		GCPProjectID:          "my-project",
		AllowedHTTPHosts:      []string{"a.example", "b.example"},
		UIBaseURL:             "https://ui.example",
		CAFile:                "/etc/ca.pem",
		ComparisonTTL:         24 * time.Hour,
		DefaultBudgetUSD:      12.5,
		HeadTargetCIHalfWidth: 0.05,
		URL:                   "https://example.com",
		ID:                    "abc-123",
		S3Bucket:              "bucket-1",
		HTTP2Enabled:          true,
	}

	if !reflect.DeepEqual(flagTarget, want) {
		t.Errorf("flagTarget mismatch: got %+v, want %+v", flagTarget, want)
	}
	if !reflect.DeepEqual(envTarget, want) {
		t.Errorf("envTarget mismatch: got %+v, want %+v", envTarget, want)
	}
	if !reflect.DeepEqual(mapTarget, want) {
		t.Errorf("mapTarget mismatch: got %+v, want %+v", mapTarget, want)
	}
}

func TestNestedAndEmbeddedStructsMultiLevel(t *testing.T) {
	type LeafOptions struct {
		CAFile  string
		BaseURL string
	}
	type MiddleOptions struct {
		LeafOptions
		Deep LeafOptions
	}
	type ServicePortalOptions struct {
		CAFile           string
		AllowedHTTPHosts []string
		ModelBaseURL     string
	}
	type RootOptions struct {
		MiddleOptions
		ServicePortal ServicePortalOptions
		Nested        MiddleOptions
	}

	mapData := map[string]string{
		"caFile":                        "/root/ca.pem",
		"baseURL":                       "https://root.example",
		"deepCAFile":                    "/deep/ca.pem",
		"deepBaseURL":                   "https://deep.example",
		"servicePortalCAFile":           "/sp/ca.pem",
		"servicePortalAllowedHTTPHosts": "proxy.example, cache.example",
		"servicePortalModelBaseURL":     "https://sp-model.example",
		"nestedCAFile":                  "/nested/ca.pem",
		"nestedBaseURL":                 "https://nested.example",
		"nestedDeepCAFile":              "/nested-deep/ca.pem",
		"nestedDeepBaseURL":             "https://nested-deep.example",
	}

	var fromMap RootOptions
	if err := options.LoadMap(&fromMap, mapData); err != nil {
		t.Fatalf("LoadMap failed: %v", err)
	}
	unknown, err := options.UnknownMapKeys(&fromMap, mapData)
	if err != nil {
		t.Fatalf("UnknownMapKeys failed: %v", err)
	}
	if len(unknown) != 0 {
		t.Fatalf("unexpected unknown keys: %v", unknown)
	}

	var fromFlags RootOptions
	flagSet := flag.NewFlagSet("nested", flag.ContinueOnError)
	if err := options.BindFlags(flagSet, &fromFlags); err != nil {
		t.Fatalf("BindFlags failed: %v", err)
	}
	flagArguments := []string{
		"--ca-file=/root/ca.pem",
		"--base-url=https://root.example",
		"--deep-ca-file=/deep/ca.pem",
		"--deep-base-url=https://deep.example",
		"--service-portal-ca-file=/sp/ca.pem",
		"--service-portal-allowed-http-hosts=proxy.example, cache.example",
		"--service-portal-model-base-url=https://sp-model.example",
		"--nested-ca-file=/nested/ca.pem",
		"--nested-base-url=https://nested.example",
		"--nested-deep-ca-file=/nested-deep/ca.pem",
		"--nested-deep-base-url=https://nested-deep.example",
	}
	if err := flagSet.Parse(flagArguments); err != nil {
		t.Fatalf("flagSet.Parse failed: %v", err)
	}

	envValues := map[string]string{
		"EVALRUNNER_CA_FILE":                           "/root/ca.pem",
		"EVALRUNNER_BASE_URL":                          "https://root.example",
		"EVALRUNNER_DEEP_CA_FILE":                      "/deep/ca.pem",
		"EVALRUNNER_DEEP_BASE_URL":                     "https://deep.example",
		"EVALRUNNER_SERVICE_PORTAL_CA_FILE":            "/sp/ca.pem",
		"EVALRUNNER_SERVICE_PORTAL_ALLOWED_HTTP_HOSTS": "proxy.example, cache.example",
		"EVALRUNNER_SERVICE_PORTAL_MODEL_BASE_URL":     "https://sp-model.example",
		"EVALRUNNER_NESTED_CA_FILE":                    "/nested/ca.pem",
		"EVALRUNNER_NESTED_BASE_URL":                   "https://nested.example",
		"EVALRUNNER_NESTED_DEEP_CA_FILE":               "/nested-deep/ca.pem",
		"EVALRUNNER_NESTED_DEEP_BASE_URL":              "https://nested-deep.example",
	}
	var fromEnv RootOptions
	if err := options.LoadEnvFrom(&fromEnv, options.DefaultEnvPrefix, mapLookup(envValues)); err != nil {
		t.Fatalf("LoadEnvFrom failed: %v", err)
	}

	want := RootOptions{
		MiddleOptions: MiddleOptions{
			LeafOptions: LeafOptions{CAFile: "/root/ca.pem", BaseURL: "https://root.example"},
			Deep:        LeafOptions{CAFile: "/deep/ca.pem", BaseURL: "https://deep.example"},
		},
		ServicePortal: ServicePortalOptions{
			CAFile:           "/sp/ca.pem",
			AllowedHTTPHosts: []string{"proxy.example", "cache.example"},
			ModelBaseURL:     "https://sp-model.example",
		},
		Nested: MiddleOptions{
			LeafOptions: LeafOptions{CAFile: "/nested/ca.pem", BaseURL: "https://nested.example"},
			Deep:        LeafOptions{CAFile: "/nested-deep/ca.pem", BaseURL: "https://nested-deep.example"},
		},
	}

	if !reflect.DeepEqual(fromMap, want) {
		t.Errorf("fromMap mismatch: got %+v, want %+v", fromMap, want)
	}
	if !reflect.DeepEqual(fromFlags, want) {
		t.Errorf("fromFlags mismatch: got %+v, want %+v", fromFlags, want)
	}
	if !reflect.DeepEqual(fromEnv, want) {
		t.Errorf("fromEnv mismatch: got %+v, want %+v", fromEnv, want)
	}
}

type ChildDefaultOptions struct {
	Host      string
	Port      int
	CallLog   []string `options:"-"`
	CallCount int      `options:"-"`
}

func (child *ChildDefaultOptions) InitDefaults() {
	child.CallCount++
	child.CallLog = append(child.CallLog, "child")
	child.Host = "child-host"
	child.Port = 8080
}

type EmbeddedDefaultOptions struct {
	Endpoint  string
	CallCount int `options:"-"`
}

func (embedded *EmbeddedDefaultOptions) InitDefaults() {
	embedded.CallCount++
	embedded.Endpoint = "embedded-default"
}

type ParentDefaultOptions struct {
	EmbeddedDefaultOptions
	Named   ChildDefaultOptions
	Skipped ChildDefaultOptions `options:"-"`
	Order   []string            `options:"-"`
}

func (parent *ParentDefaultOptions) InitDefaults() {
	parent.Order = append(parent.Order, fmt.Sprintf("named-was-%s:%d", parent.Named.Host, parent.Named.Port))
	parent.Order = append(parent.Order, fmt.Sprintf("embedded-was-%s", parent.Endpoint))
	parent.Named.Port = 9090
	parent.Endpoint = "parent-overridden"
}

type WrapperWithoutOwnInitDefaults struct {
	EmbeddedDefaultOptions
}

type DeepWrapperWithoutOwnInitDefaults struct {
	WrapperWithoutOwnInitDefaults
}

type unexportedEmbeddedDefaults struct {
	Value     string
	CallCount int `options:"-"`
}

func (embedded *unexportedEmbeddedDefaults) InitDefaults() {
	embedded.CallCount++
	embedded.Value = "from-unexported-embedded"
}

type WrapperWithUnexportedEmbedded struct {
	unexportedEmbeddedDefaults
}

func TestInitDefaultsOrderAndPromotion(t *testing.T) {
	t.Run("children before parent allows parent override", func(t *testing.T) {
		var target ParentDefaultOptions
		if err := options.InitDefaults(&target); err != nil {
			t.Fatalf("InitDefaults failed: %v", err)
		}
		if target.Named.Host != "child-host" {
			t.Errorf("Named.Host = %q, want %q", target.Named.Host, "child-host")
		}
		if target.Named.Port != 9090 {
			t.Errorf("Named.Port = %d, want 9090 (overridden by parent)", target.Named.Port)
		}
		if target.Endpoint != "parent-overridden" {
			t.Errorf("Endpoint = %q, want %q", target.Endpoint, "parent-overridden")
		}
		if target.Named.CallCount != 1 {
			t.Errorf("Named.CallCount = %d, want 1", target.Named.CallCount)
		}
		if target.EmbeddedDefaultOptions.CallCount != 1 {
			t.Errorf("EmbeddedDefaultOptions.CallCount = %d, want 1", target.EmbeddedDefaultOptions.CallCount)
		}
		if target.Skipped.CallCount != 0 {
			t.Errorf(`Skipped.CallCount = %d, want 0 (options:"-" must be skipped)`, target.Skipped.CallCount)
		}
		wantOrder := []string{"named-was-child-host:8080", "embedded-was-embedded-default"}
		if !reflect.DeepEqual(target.Order, wantOrder) {
			t.Errorf("Order = %v, want %v", target.Order, wantOrder)
		}
	})

	t.Run("promoted embedded InitDefaults is called only once", func(t *testing.T) {
		var wrapper WrapperWithoutOwnInitDefaults
		if err := options.InitDefaults(&wrapper); err != nil {
			t.Fatalf("InitDefaults failed: %v", err)
		}
		if wrapper.Endpoint != "embedded-default" {
			t.Errorf("Endpoint = %q, want %q", wrapper.Endpoint, "embedded-default")
		}
		if wrapper.CallCount != 1 {
			t.Errorf("CallCount = %d, want 1 (promoted InitDefaults must not run twice)", wrapper.CallCount)
		}
	})

	t.Run("multi-level promoted embedded InitDefaults is called only once", func(t *testing.T) {
		var deep DeepWrapperWithoutOwnInitDefaults
		if err := options.InitDefaults(&deep); err != nil {
			t.Fatalf("InitDefaults failed: %v", err)
		}
		if deep.Endpoint != "embedded-default" {
			t.Errorf("Endpoint = %q, want %q", deep.Endpoint, "embedded-default")
		}
		if deep.CallCount != 1 {
			t.Errorf("CallCount = %d, want 1 (multi-level promoted InitDefaults must not run twice)", deep.CallCount)
		}
	})

	t.Run("unexported embedded InitDefaults runs via promotion", func(t *testing.T) {
		var wrapper WrapperWithUnexportedEmbedded
		if err := options.InitDefaults(&wrapper); err != nil {
			t.Fatalf("InitDefaults failed: %v", err)
		}
		if wrapper.Value != "from-unexported-embedded" {
			t.Errorf("Value = %q, want %q", wrapper.Value, "from-unexported-embedded")
		}
		if wrapper.CallCount != 1 {
			t.Errorf("CallCount = %d, want 1", wrapper.CallCount)
		}
	})
}

func TestAllSupportedTypesAndPrintDefaults(t *testing.T) {
	type AllTypesOptions struct {
		Name        string        `desc:"service display name"`
		Enabled     bool          `desc:"enable feature"`
		Workers     int           `desc:"number of workers"`
		MaxBytes    int64         `desc:"maximum upload size in bytes"`
		Ratio       float64       `desc:"target ratio"`
		Timeout     time.Duration `desc:"request timeout"`
		Hosts       []string      `desc:"allowed hosts"`
		BindAddress netip.Addr    `desc:"listen IP address"`
		Mode        modeValue     `desc:"affinity mode"`
		Token       opaqueToken   `desc:"opaque auth token"`
		Ignored     chan int      `options:"-"`
		unexported  int
	}

	target := AllTypesOptions{
		Name:        "default-service",
		Enabled:     true,
		Workers:     4,
		MaxBytes:    1024,
		Ratio:       0.25,
		Timeout:     30 * time.Second,
		Hosts:       []string{"one.example", "two.example"},
		BindAddress: netip.MustParseAddr("127.0.0.1"),
		Mode:        modeValue{value: "order"},
		unexported:  99,
	}

	flagSet := flag.NewFlagSet("all-types", flag.ContinueOnError)
	if err := options.BindFlags(flagSet, &target); err != nil {
		t.Fatalf("BindFlags failed: %v", err)
	}

	// Verify descriptions and PrintDefaults output (also exercises zero-value String() safety).
	var output bytes.Buffer
	flagSet.SetOutput(&output)
	flagSet.PrintDefaults()
	usageText := output.String()

	for _, expectedSnippet := range []string{
		"service display name",
		"enable feature",
		"number of workers",
		"maximum upload size in bytes",
		"target ratio",
		"request timeout",
		"allowed hosts",
		"one.example,two.example",
		"listen IP address",
		"127.0.0.1",
		"affinity mode",
		"order",
		"opaque auth token",
	} {
		if !strings.Contains(usageText, expectedSnippet) {
			t.Errorf("PrintDefaults output missing %q: %s", expectedSnippet, usageText)
		}
	}
	if flagSet.Lookup("ignored") != nil {
		t.Error(`expected options:"-" field not to be bound as flag`)
	}
	if flagSet.Lookup("unexported") != nil {
		t.Error("expected unexported field not to be bound as flag")
	}

	// Parse values for every supported type.
	arguments := []string{
		"--name=updated-service",
		"--enabled=false",
		"--workers=16",
		"--max-bytes=9876543210",
		"--ratio=0.75",
		"--timeout=2m30s",
		"--hosts= alpha.example , beta.example , ",
		"--bind-address=10.0.0.5",
		"--mode=prune",
		"--token=secret-value",
	}
	if err := flagSet.Parse(arguments); err != nil {
		t.Fatalf("flagSet.Parse failed: %v", err)
	}

	if target.Name != "updated-service" ||
		target.Enabled != false ||
		target.Workers != 16 ||
		target.MaxBytes != 9876543210 ||
		target.Ratio != 0.75 ||
		target.Timeout != 150*time.Second ||
		!reflect.DeepEqual(target.Hosts, []string{"alpha.example", "beta.example"}) ||
		target.BindAddress != netip.MustParseAddr("10.0.0.5") ||
		target.Mode.value != "prune" ||
		target.Token.secret != "secret-value" ||
		target.unexported != 99 {
		t.Errorf("unexpected parsed target: %+v", target)
	}
}

func TestStringSliceNoneSentinelAndMapEmptyHandling(t *testing.T) {
	type SliceOptions struct {
		Hosts []string
		Name  string
		Count int
	}

	t.Run("LoadMap skips empty and whitespace values preserving defaults", func(t *testing.T) {
		target := SliceOptions{
			Hosts: []string{"default.example"},
			Name:  "default-name",
			Count: 10,
		}
		err := options.LoadMap(&target, map[string]string{
			"hosts": "   \t  ",
			"name":  "",
			"count": "  ",
		})
		if err != nil {
			t.Fatalf("LoadMap failed: %v", err)
		}
		if !reflect.DeepEqual(target.Hosts, []string{"default.example"}) {
			t.Errorf("Hosts = %v, want default preserved", target.Hosts)
		}
		if target.Name != "default-name" {
			t.Errorf("Name = %q, want default preserved", target.Name)
		}
		if target.Count != 10 {
			t.Errorf("Count = %d, want default preserved", target.Count)
		}
	})

	for _, sentinel := range []string{"none", "NONE", "None", "  NoNe  "} {
		t.Run("LoadMap clears slice with "+sentinel, func(t *testing.T) {
			target := SliceOptions{Hosts: []string{"a.example", "b.example"}}
			if err := options.LoadMap(&target, map[string]string{"hosts": sentinel}); err != nil {
				t.Fatalf("LoadMap failed: %v", err)
			}
			if len(target.Hosts) != 0 {
				t.Errorf("Hosts = %v, want empty/nil after %q", target.Hosts, sentinel)
			}
		})

		t.Run("LoadEnv clears slice with "+sentinel, func(t *testing.T) {
			target := SliceOptions{Hosts: []string{"a.example", "b.example"}}
			err := options.LoadEnvFrom(&target, options.DefaultEnvPrefix, mapLookup(map[string]string{
				"EVALRUNNER_HOSTS": sentinel,
			}))
			if err != nil {
				t.Fatalf("LoadEnvFrom failed: %v", err)
			}
			if len(target.Hosts) != 0 {
				t.Errorf("Hosts = %v, want empty/nil after %q", target.Hosts, sentinel)
			}
		})

		t.Run("Flag clears slice with "+sentinel, func(t *testing.T) {
			target := SliceOptions{Hosts: []string{"a.example", "b.example"}}
			flagSet := flag.NewFlagSet("slice", flag.ContinueOnError)
			if err := options.BindFlags(flagSet, &target); err != nil {
				t.Fatalf("BindFlags failed: %v", err)
			}
			if err := flagSet.Parse([]string{"--hosts=" + sentinel}); err != nil {
				t.Fatalf("flagSet.Parse failed: %v", err)
			}
			if len(target.Hosts) != 0 {
				t.Errorf("Hosts = %v, want empty/nil after %q", target.Hosts, sentinel)
			}
		})
	}
}

func TestUnknownMapKeys(t *testing.T) {
	type Inner struct {
		BaseURL string
	}
	type SampleOptions struct {
		FooBar  string
		Inner   Inner
		Ignored string `options:"-"`
	}

	var target SampleOptions
	unknown, err := options.UnknownMapKeys(&target, map[string]string{
		"fooBar":       "ok",
		"innerBaseURL": "ok",
		"zetaKey":      "unknown",
		"alphaKey":     "unknown",
		"ignored":      "unknown because ignored",
	})
	if err != nil {
		t.Fatalf("UnknownMapKeys failed: %v", err)
	}
	want := []string{"alphaKey", "ignored", "zetaKey"}
	if !reflect.DeepEqual(unknown, want) {
		t.Errorf("UnknownMapKeys = %v, want %v", unknown, want)
	}

	noneUnknown, err := options.UnknownMapKeys(&target, map[string]string{
		"fooBar":       "ok",
		"innerBaseURL": "ok",
	})
	if err != nil {
		t.Fatalf("UnknownMapKeys failed: %v", err)
	}
	if noneUnknown != nil {
		t.Errorf("expected nil when all keys are known, got %v", noneUnknown)
	}

	nilDataUnknown, err := options.UnknownMapKeys(&target, nil)
	if err != nil {
		t.Fatalf("UnknownMapKeys failed: %v", err)
	}
	if nilDataUnknown != nil {
		t.Errorf("expected nil for nil map, got %v", nilDataUnknown)
	}
}

type precedenceOptions struct {
	OnlyDefault        string
	FromMap            string
	FromEnv            string
	FromFlag           string
	FlagMatchesDefault int
	BoolFlagFalse      bool
	EmptyStringFlag    string
	HostsClearedFlag   []string
	OpaqueFromFlag     opaqueToken
	PreservedExternal  string `options:"-"`
}

func (cfg *precedenceOptions) InitDefaults() {
	cfg.OnlyDefault = "default-only"
	cfg.FromMap = "default-map"
	cfg.FromEnv = "default-env"
	cfg.FromFlag = "default-flag"
	cfg.FlagMatchesDefault = 8080
	cfg.BoolFlagFalse = true
	cfg.EmptyStringFlag = "default-non-empty"
	cfg.HostsClearedFlag = []string{"default.example"}
}

func TestFourLayerPrecedenceAndLiveReload(t *testing.T) {
	initialMap := map[string]string{
		"fromMap":            "map-value",
		"fromEnv":            "map-overridden-by-env",
		"fromFlag":           "map-overridden-by-flag",
		"flagMatchesDefault": "9000",
		"boolFlagFalse":      "true",
		"emptyStringFlag":    "map-non-empty",
		"hostsClearedFlag":   "map.example",
		"opaqueFromFlag":     "from-map",
	}
	envValues := map[string]string{
		"EVALRUNNER_FROM_ENV":             "env-value",
		"EVALRUNNER_FROM_FLAG":            "env-overridden-by-flag",
		"EVALRUNNER_FLAG_MATCHES_DEFAULT": "7000",
		"EVALRUNNER_BOOL_FLAG_FALSE":      "true",
		"EVALRUNNER_EMPTY_STRING_FLAG":    "env-non-empty",
		"EVALRUNNER_HOSTS_CLEARED_FLAG":   "env.example",
		"EVALRUNNER_OPAQUE_FROM_FLAG":     "from-env",
	}
	commandLineArguments := []string{
		"--extra-unrelated-flag=hello",
		"--from-flag=flag-value",
		"--flag-matches-default=8080",
		"--bool-flag-false=false",
		"--empty-string-flag=",
		"--hosts-cleared-flag=none",
		"--opaque-from-flag=flag-secret",
	}

	flagSet := flag.NewFlagSet("precedence", flag.ContinueOnError)
	extraFlag := flagSet.String("extra-unrelated-flag", "", "unrelated CLI flag")

	target := precedenceOptions{PreservedExternal: "keep-me"}
	err := options.Parse(
		flagSet,
		commandLineArguments,
		&target,
		options.WithMap(initialMap),
		options.WithLookupEnv(mapLookup(envValues)),
	)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if *extraFlag != "hello" {
		t.Errorf("extra-unrelated-flag = %q, want %q", *extraFlag, "hello")
	}

	wantInitial := precedenceOptions{
		OnlyDefault:        "default-only",
		FromMap:            "map-value",
		FromEnv:            "env-value",
		FromFlag:           "flag-value",
		FlagMatchesDefault: 8080,
		BoolFlagFalse:      false,
		EmptyStringFlag:    "",
		HostsClearedFlag:   nil,
		OpaqueFromFlag:     opaqueToken{secret: "flag-secret"},
		PreservedExternal:  "keep-me",
	}
	if !reflect.DeepEqual(target, wantInitial) {
		t.Fatalf("initial Parse mismatch: got %+v, want %+v", target, wantInitial)
	}

	// Live reload #1: update ConfigMap (change fromMap, try to override flag/env, add onlyDefault).
	updatedMap := map[string]string{
		"onlyDefault":      "now-from-map",
		"fromMap":          "map-value-v2",
		"fromEnv":          "map-v2-still-loses-to-env",
		"fromFlag":         "map-v2-still-loses-to-flag",
		"hostsClearedFlag": "map-v2.example",
		"opaqueFromFlag":   "map-v2-secret",
	}
	if err := options.Resolve(
		&target,
		options.WithFlagSet(flagSet),
		options.WithMap(updatedMap),
		options.WithLookupEnv(mapLookup(envValues)),
	); err != nil {
		t.Fatalf("live reload #1 failed: %v", err)
	}
	wantReload1 := wantInitial
	wantReload1.OnlyDefault = "now-from-map"
	wantReload1.FromMap = "map-value-v2"
	if !reflect.DeepEqual(target, wantReload1) {
		t.Fatalf("live reload #1 mismatch: got %+v, want %+v", target, wantReload1)
	}

	// Live reload #2: malformed ConfigMap value must fail and leave target unmodified.
	malformedMap := map[string]string{
		"onlyDefault":        "should-not-apply",
		"flagMatchesDefault": "not-an-integer",
	}
	if err := options.Resolve(
		&target,
		options.WithFlagSet(flagSet),
		options.WithMap(malformedMap),
		options.WithLookupEnv(mapLookup(envValues)),
	); err == nil {
		t.Fatal("expected error for malformed ConfigMap update, got nil")
	}
	if !reflect.DeepEqual(target, wantReload1) {
		t.Fatalf("target was mutated on failed Resolve: got %+v, want %+v", target, wantReload1)
	}

	// Live reload #3: ConfigMap deleted (nil map) -> map-only keys revert to defaults while env and explicit flags stay preserved.
	if err := options.Resolve(
		&target,
		options.WithFlagSet(flagSet),
		options.WithMap(nil),
		options.WithLookupEnv(mapLookup(envValues)),
	); err != nil {
		t.Fatalf("live reload #3 failed: %v", err)
	}
	wantReload3 := wantInitial
	wantReload3.OnlyDefault = "default-only"
	wantReload3.FromMap = "default-map"
	if !reflect.DeepEqual(target, wantReload3) {
		t.Fatalf("live reload #3 mismatch: got %+v, want %+v", target, wantReload3)
	}

	// Live reload #4: Resolve into a brand-new struct instance using the same parsed flagSet.
	var freshTarget precedenceOptions
	if err := options.Resolve(
		&freshTarget,
		options.WithFlagSet(flagSet),
		options.WithMap(map[string]string{"fromMap": "fresh-map"}),
		options.WithLookupEnv(mapLookup(envValues)),
	); err != nil {
		t.Fatalf("live reload into fresh target failed: %v", err)
	}
	wantFresh := wantInitial
	wantFresh.FromMap = "fresh-map"
	wantFresh.PreservedExternal = ""
	if !reflect.DeepEqual(freshTarget, wantFresh) {
		t.Fatalf("fresh target Resolve mismatch: got %+v, want %+v", freshTarget, wantFresh)
	}
}

func TestLoadEnvAndResolveOptions(t *testing.T) {
	type EnvOptions struct {
		FooBar string
	}

	t.Setenv("EVALRUNNER_FOO_BAR", "from-process-env")

	var viaLoadEnv EnvOptions
	if err := options.LoadEnv(&viaLoadEnv, options.DefaultEnvPrefix); err != nil {
		t.Fatalf("LoadEnv failed: %v", err)
	}
	if viaLoadEnv.FooBar != "from-process-env" {
		t.Errorf("FooBar = %q, want %q", viaLoadEnv.FooBar, "from-process-env")
	}

	var withoutEnv EnvOptions
	if err := options.Resolve(&withoutEnv, options.WithoutEnv()); err != nil {
		t.Fatalf("Resolve with WithoutEnv failed: %v", err)
	}
	if withoutEnv.FooBar != "" {
		t.Errorf("FooBar = %q, want empty when WithoutEnv is used", withoutEnv.FooBar)
	}

	var customPrefix EnvOptions
	if err := options.Resolve(
		&customPrefix,
		options.WithEnvPrefix("CUSTOM_"),
		options.WithLookupEnv(mapLookup(map[string]string{"CUSTOM_FOO_BAR": "from-custom-prefix"})),
	); err != nil {
		t.Fatalf("Resolve with custom prefix failed: %v", err)
	}
	if customPrefix.FooBar != "from-custom-prefix" {
		t.Errorf("FooBar = %q, want %q", customPrefix.FooBar, "from-custom-prefix")
	}

	var emptyPrefix EnvOptions
	if err := options.Resolve(
		&emptyPrefix,
		options.WithEnvPrefix(""),
		options.WithLookupEnv(mapLookup(map[string]string{"FOO_BAR": "from-empty-prefix"})),
	); err != nil {
		t.Fatalf("Resolve with empty prefix failed: %v", err)
	}
	if emptyPrefix.FooBar != "from-empty-prefix" {
		t.Errorf("FooBar = %q, want %q", emptyPrefix.FooBar, "from-empty-prefix")
	}
}

func TestErrorCases(t *testing.T) {
	type SimpleOptions struct {
		Count    int
		BigCount int64
		Ratio    float64
		Enabled  bool
		Timeout  time.Duration
		Mode     modeValue
	}

	t.Run("invalid targets", func(t *testing.T) {
		var nilPointer *SimpleOptions
		nonStruct := 123
		invalidTargets := []struct {
			name   string
			target any
		}{
			{name: "nil interface", target: nil},
			{name: "non-pointer struct value", target: SimpleOptions{}},
			{name: "nil struct pointer", target: nilPointer},
			{name: "pointer to non-struct", target: &nonStruct},
		}

		for _, testCase := range invalidTargets {
			t.Run(testCase.name, func(t *testing.T) {
				if err := options.InitDefaults(testCase.target); err == nil {
					t.Errorf("InitDefaults(%s): expected error, got nil", testCase.name)
				}
				flagSet := flag.NewFlagSet("err", flag.ContinueOnError)
				if err := options.BindFlags(flagSet, testCase.target); err == nil {
					t.Errorf("BindFlags(%s): expected error, got nil", testCase.name)
				}
				if err := options.LoadEnvFrom(testCase.target, options.DefaultEnvPrefix, mapLookup(nil)); err == nil {
					t.Errorf("LoadEnvFrom(%s): expected error, got nil", testCase.name)
				}
				if err := options.LoadMap(testCase.target, nil); err == nil {
					t.Errorf("LoadMap(%s): expected error, got nil", testCase.name)
				}
				if _, err := options.UnknownMapKeys(testCase.target, nil); err == nil {
					t.Errorf("UnknownMapKeys(%s): expected error, got nil", testCase.name)
				}
				if err := options.Resolve(testCase.target); err == nil {
					t.Errorf("Resolve(%s): expected error, got nil", testCase.name)
				}
			})
		}
	})

	t.Run("nil flagSet", func(t *testing.T) {
		var target SimpleOptions
		if err := options.BindFlags(nil, &target); err == nil {
			t.Error("BindFlags(nil, ...): expected error, got nil")
		}
		if err := options.Parse(nil, nil, &target); err == nil {
			t.Error("Parse(nil, ...): expected error, got nil")
		}
	})

	t.Run("flag already registered on flagSet", func(t *testing.T) {
		var target SimpleOptions
		flagSet := flag.NewFlagSet("dup-flag", flag.ContinueOnError)
		flagSet.Int("count", 0, "pre-registered")
		if err := options.BindFlags(flagSet, &target); err == nil {
			t.Error("expected error when flag is already registered on flagSet")
		}
	})

	t.Run("unsupported field types", func(t *testing.T) {
		type EmbeddedTarget struct{ Value string }

		cases := []struct {
			name   string
			target any
		}{
			{name: "uint", target: &struct{ Value uint }{}},
			{name: "int32", target: &struct{ Value int32 }{}},
			{name: "pointer to int", target: &struct{ Value *int }{}},
			{name: "slice of int", target: &struct{ Value []int }{}},
			{name: "map", target: &struct{ Value map[string]string }{}},
			{name: "chan", target: &struct{ Value chan int }{}},
			{name: "embedded pointer to struct", target: &struct{ *EmbeddedTarget }{}},
			{name: "unsupported options tag", target: &struct {
				Value string `options:"unknown"`
			}{}},
		}

		for _, testCase := range cases {
			t.Run(testCase.name, func(t *testing.T) {
				flagSet := flag.NewFlagSet("unsupported", flag.ContinueOnError)
				if err := options.BindFlags(flagSet, testCase.target); err == nil {
					t.Errorf("BindFlags(%s): expected error, got nil", testCase.name)
				}
				if err := options.LoadMap(testCase.target, nil); err == nil {
					t.Errorf("LoadMap(%s): expected error, got nil", testCase.name)
				}
			})
		}
	})

	t.Run("duplicate derived names", func(t *testing.T) {
		type EmbeddedA struct{ Shared string }
		type EmbeddedB struct{ Shared string }
		type NestedFoo struct{ Bar string }

		cases := []struct {
			name   string
			target any
		}{
			{
				name: "duplicate across anonymous embedded structs",
				target: &struct {
					EmbeddedA
					EmbeddedB
				}{},
			},
			{
				name: "nested struct collides with flat field",
				target: &struct {
					Foo    NestedFoo
					FooBar string
				}{},
			},
			{
				name: "case collision on acronym",
				target: &struct {
					URL string
					Url string
				}{},
			},
		}

		for _, testCase := range cases {
			t.Run(testCase.name, func(t *testing.T) {
				if err := options.LoadMap(testCase.target, nil); err == nil {
					t.Errorf("LoadMap(%s): expected duplicate name error, got nil", testCase.name)
				}
			})
		}
	})

	t.Run("malformed values", func(t *testing.T) {
		cases := []struct {
			name     string
			mapKey   string
			envName  string
			flagName string
			badValue string
		}{
			{name: "bad int", mapKey: "count", envName: "EVALRUNNER_COUNT", flagName: "count", badValue: "abc"},
			{name: "bad int64", mapKey: "bigCount", envName: "EVALRUNNER_BIG_COUNT", flagName: "big-count", badValue: "not64"},
			{name: "bad float64", mapKey: "ratio", envName: "EVALRUNNER_RATIO", flagName: "ratio", badValue: "notfloat"},
			{name: "bad bool", mapKey: "enabled", envName: "EVALRUNNER_ENABLED", flagName: "enabled", badValue: "maybe"},
			{name: "bad duration", mapKey: "timeout", envName: "EVALRUNNER_TIMEOUT", flagName: "timeout", badValue: "10lightyears"},
			{name: "bad TextUnmarshaler", mapKey: "mode", envName: "EVALRUNNER_MODE", flagName: "mode", badValue: "invalid-mode"},
		}

		for _, testCase := range cases {
			t.Run(testCase.name, func(t *testing.T) {
				var mapTarget SimpleOptions
				if err := options.LoadMap(&mapTarget, map[string]string{testCase.mapKey: testCase.badValue}); err == nil {
					t.Errorf("LoadMap(%s): expected error, got nil", testCase.name)
				}

				var envTarget SimpleOptions
				if err := options.LoadEnvFrom(&envTarget, options.DefaultEnvPrefix, mapLookup(map[string]string{
					testCase.envName: testCase.badValue,
				})); err == nil {
					t.Errorf("LoadEnvFrom(%s): expected error, got nil", testCase.name)
				}

				var flagTarget SimpleOptions
				flagSet := flag.NewFlagSet("malformed", flag.ContinueOnError)
				flagSet.SetOutput(&bytes.Buffer{})
				if err := options.BindFlags(flagSet, &flagTarget); err != nil {
					t.Fatalf("BindFlags failed: %v", err)
				}
				if err := flagSet.Parse([]string{fmt.Sprintf("--%s=%s", testCase.flagName, testCase.badValue)}); err == nil {
					t.Errorf("flagSet.Parse(%s): expected error, got nil", testCase.name)
				}
			})
		}
	})
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
