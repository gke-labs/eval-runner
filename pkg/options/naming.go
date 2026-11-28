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

package options

import (
	"strings"
	"unicode"
)

// splitWords splits a Go identifier into words while preserving acronyms.
//
// Examples:
//   - "Bar" -> ["Bar"]
//   - "FooBar" -> ["Foo", "Bar"]
//   - "ModelBaseURL" -> ["Model", "Base", "URL"]
//   - "GCPProjectID" -> ["GCP", "Project", "ID"]
//   - "AllowedHTTPHosts" -> ["Allowed", "HTTP", "Hosts"]
//   - "UIBaseURL" -> ["UI", "Base", "URL"]
func splitWords(identifier string) []string {
	if identifier == "" {
		return nil
	}

	var words []string
	for _, segment := range strings.Split(identifier, "_") {
		if segment == "" {
			continue
		}
		runes := []rune(segment)
		wordStart := 0
		for i := 1; i < len(runes); i++ {
			previous := runes[i-1]
			current := runes[i]
			startsNewWord := false
			if unicode.IsUpper(current) {
				if !unicode.IsUpper(previous) {
					startsNewWord = true
				} else if i+1 < len(runes) && unicode.IsLower(runes[i+1]) {
					startsNewWord = true
				}
			}
			if startsNewWord {
				words = append(words, string(runes[wordStart:i]))
				wordStart = i
			}
		}
		words = append(words, string(runes[wordStart:]))
	}
	return words
}

// formatFlagName formats split words as kebab-case (e.g. "model-base-url").
func formatFlagName(words []string) string {
	lowered := make([]string, len(words))
	for i, word := range words {
		lowered[i] = strings.ToLower(word)
	}
	return strings.Join(lowered, "-")
}

// formatEnvSuffix formats split words as SCREAMING_SNAKE_CASE (e.g. "MODEL_BASE_URL").
func formatEnvSuffix(words []string) string {
	uppered := make([]string, len(words))
	for i, word := range words {
		uppered[i] = strings.ToUpper(word)
	}
	return strings.Join(uppered, "_")
}

// formatMapKey formats split words as lowerCamelCase, lowercasing leading words
// (including leading acronyms like "GCP" -> "gcp") while preserving uppercase
// acronyms in subsequent words (e.g. "gcpProjectID", "allowedHTTPHosts", "uiBaseURL").
func formatMapKey(words []string) string {
	if len(words) == 0 {
		return ""
	}
	var builder strings.Builder
	builder.WriteString(strings.ToLower(words[0]))
	for _, word := range words[1:] {
		builder.WriteString(word)
	}
	return builder.String()
}
