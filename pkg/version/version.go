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
	"runtime/debug"
	"strings"
)

// Version is the build-time version string set via ldflags:
//
//	-ldflags "-X github.com/gke-labs/eval-runner/pkg/version.Version=$(git describe ...)"
var Version = ""

// Get returns the effective version of the binary.
// If Version was injected via ldflags, it is returned.
// Otherwise, it falls back to Go runtime/debug build info (Main.Version, or vcs.revision).
// If neither is available, it returns "devel".
func Get() string {
	if v := strings.TrimSpace(Version); v != "" {
		return v
	}

	if info, ok := debug.ReadBuildInfo(); ok {
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			return info.Main.Version
		}
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && setting.Value != "" {
				rev := setting.Value
				if len(rev) > 12 {
					rev = rev[:12]
				}
				for _, s := range info.Settings {
					if s.Key == "vcs.modified" && s.Value == "true" {
						return rev + "-dirty"
					}
				}
				return rev
			}
		}
	}

	return "devel"
}
