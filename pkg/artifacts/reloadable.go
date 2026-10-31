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

package artifacts

import (
	"context"
	"errors"
	"sync"
)

// ErrSinkClosed is returned when an operation is attempted on a closed ReloadableSink.
var ErrSinkClosed = errors.New("artifact sink is closed")

// ErrNoSinkConfigured is returned when a storage operation is attempted without an active sink.
var ErrNoSinkConfigured = errors.New("no artifact sink configured")

// trackedSink pairs an ArtifactSink with a WaitGroup tracking active in-flight operations.
type trackedSink struct {
	sink ArtifactSink
	wg   sync.WaitGroup
}

// ReloadableSink wraps an ArtifactSink, allowing runtime replacement (hot-reloading)
// of the underlying sink. In-flight operations on the old sink are allowed to finish
// before the old sink is closed.
type ReloadableSink struct {
	mu             sync.RWMutex
	currentTracked *trackedSink
	closed         bool
	onCloseError   func(error)
}

// NewReloadableSink creates a new ReloadableSink wrapping the initial sink.
func NewReloadableSink(initial ArtifactSink) *ReloadableSink {
	s := &ReloadableSink{}
	if initial != nil {
		s.currentTracked = &trackedSink{sink: initial}
	}
	return s
}

// SetOnCloseError registers a callback invoked if closing a retired sink returns an error.
func (s *ReloadableSink) SetOnCloseError(fn func(error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onCloseError = fn
}

// Current returns the active underlying ArtifactSink, or nil if none is configured.
func (s *ReloadableSink) Current() ArtifactSink {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.currentTracked != nil {
		return s.currentTracked.sink
	}
	return nil
}

// Swap replaces the current underlying sink with newSink.
// In-flight operations on the previous sink are allowed to finish in the background;
// once they complete, the previous sink's Close() method is called.
func (s *ReloadableSink) Swap(newSink ArtifactSink) {
	if err := s.swapInternal(newSink, false); err != nil {
		s.reportCloseError(err)
	}
}

// SwapWait replaces the current underlying sink with newSink and synchronously waits
// for all in-flight operations on the old sink to complete and the old sink to close.
// It returns any error encountered while closing the previous sink.
func (s *ReloadableSink) SwapWait(newSink ArtifactSink) error {
	return s.swapInternal(newSink, true)
}

func (s *ReloadableSink) reportCloseError(err error) {
	if err == nil {
		return
	}
	s.mu.RLock()
	onErr := s.onCloseError
	s.mu.RUnlock()
	if onErr != nil {
		onErr(err)
	}
}

func (s *ReloadableSink) swapInternal(newSink ArtifactSink, wait bool) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		if newSink != nil {
			if err := newSink.Close(); err != nil {
				return err
			}
		}
		return nil
	}
	oldTracked := s.currentTracked
	if newSink != nil {
		s.currentTracked = &trackedSink{sink: newSink}
	} else {
		s.currentTracked = nil
	}
	s.mu.Unlock()

	var closeErr error
	if oldTracked != nil && oldTracked.sink != nil {
		closeOld := func() error {
			oldTracked.wg.Wait()
			if err := oldTracked.sink.Close(); err != nil {
				s.reportCloseError(err)
				return err
			}
			return nil
		}
		if wait {
			closeErr = closeOld()
		} else {
			go func() {
				if err := closeOld(); err != nil {
					// closeOld already reports error via reportCloseError
				}
			}()
		}
	}
	return closeErr
}

// StoreArtifact persists the raw log bytes for a task using the current active sink.
func (s *ReloadableSink) StoreArtifact(ctx context.Context, meta RunMetadata, filename string, data []byte) (string, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return "", ErrSinkClosed
	}
	ts := s.currentTracked
	if ts == nil || ts.sink == nil {
		s.mu.RUnlock()
		return "", ErrNoSinkConfigured
	}
	ts.wg.Add(1)
	s.mu.RUnlock()
	defer ts.wg.Done()

	return ts.sink.StoreArtifact(ctx, meta, filename, data)
}

// StoreComparison persists comparison summary data using the current active sink.
func (s *ReloadableSink) StoreComparison(ctx context.Context, comparisonName string, data []byte) (string, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return "", ErrSinkClosed
	}
	ts := s.currentTracked
	if ts == nil || ts.sink == nil {
		s.mu.RUnlock()
		return "", ErrNoSinkConfigured
	}
	ts.wg.Add(1)
	s.mu.RUnlock()
	defer ts.wg.Done()

	return ts.sink.StoreComparison(ctx, comparisonName, data)
}

// StoreManifest persists the discovered manifest JSON using the current active sink.
func (s *ReloadableSink) StoreManifest(ctx context.Context, meta RunMetadata, data []byte) (string, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return "", ErrSinkClosed
	}
	ts := s.currentTracked
	if ts == nil || ts.sink == nil {
		s.mu.RUnlock()
		return "", ErrNoSinkConfigured
	}
	ts.wg.Add(1)
	s.mu.RUnlock()
	defer ts.wg.Done()

	return ts.sink.StoreManifest(ctx, meta, data)
}

// Load retrieves raw artifact bytes from the given storage path using the current active sink.
func (s *ReloadableSink) Load(ctx context.Context, path string) ([]byte, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, ErrSinkClosed
	}
	ts := s.currentTracked
	if ts == nil || ts.sink == nil {
		s.mu.RUnlock()
		return nil, ErrNotFound
	}
	ts.wg.Add(1)
	s.mu.RUnlock()
	defer ts.wg.Done()

	return ts.sink.Load(ctx, path)
}

// ArtifactPrefix returns the run-level storage prefix for a given commit from the current active sink.
func (s *ReloadableSink) ArtifactPrefix(commit string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.currentTracked != nil && s.currentTracked.sink != nil {
		return s.currentTracked.sink.ArtifactPrefix(commit)
	}
	return ""
}

// TaskArtifactPath implements TaskArtifactPather for ReloadableSink.
func (s *ReloadableSink) TaskArtifactPath(commit, taskID, filename string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.currentTracked != nil && s.currentTracked.sink != nil {
		if pather, ok := s.currentTracked.sink.(TaskArtifactPather); ok {
			return pather.TaskArtifactPath(commit, taskID, filename)
		}
	}
	return ""
}

// IsDurable reports whether the current active sink provides durable persistent storage.
func (s *ReloadableSink) IsDurable() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.currentTracked != nil && s.currentTracked.sink != nil {
		return s.currentTracked.sink.IsDurable()
	}
	return false
}

// Close releases the current sink after waiting for all in-flight operations to finish.
func (s *ReloadableSink) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	ts := s.currentTracked
	s.currentTracked = nil
	s.mu.Unlock()

	if ts != nil && ts.sink != nil {
		ts.wg.Wait()
		return ts.sink.Close()
	}
	return nil
}

// Bucket returns the bucket name if the current sink provides a Bucket() method.
func (s *ReloadableSink) Bucket() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.currentTracked != nil && s.currentTracked.sink != nil {
		type bucketGetter interface {
			Bucket() string
		}
		if bg, ok := s.currentTracked.sink.(bucketGetter); ok {
			return bg.Bucket()
		}
	}
	return ""
}

// BaseDir returns the base directory if the current sink is a LocalDirSink or provides a BaseDir() method.
func (s *ReloadableSink) BaseDir() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.currentTracked != nil && s.currentTracked.sink != nil {
		if ld, ok := s.currentTracked.sink.(*LocalDirSink); ok {
			return ld.BaseDir
		}
		type dirGetter interface {
			BaseDir() string
		}
		if dg, ok := s.currentTracked.sink.(dirGetter); ok {
			return dg.BaseDir()
		}
	}
	return ""
}
