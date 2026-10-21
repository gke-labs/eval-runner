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

package version

import (
	"testing"
)

func TestGet_Injected(t *testing.T) {
	orig := Version
	defer func() { Version = orig }()

	Version = "v1.2.3"
	if got := Get(); got != "v1.2.3" {
		t.Errorf("Expected v1.2.3, got %q", got)
	}

	Version = "  v2.0.0-rc1  "
	if got := Get(); got != "v2.0.0-rc1" {
		t.Errorf("Expected trimmed v2.0.0-rc1, got %q", got)
	}
}

func TestGet_Fallback(t *testing.T) {
	orig := Version
	defer func() { Version = orig }()

	Version = ""
	got := Get()
	if got == "" {
		t.Errorf("Expected non-empty fallback version, got empty string")
	}
}
