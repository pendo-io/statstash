// Copyright 2014 pendo.io
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package mock

import (
	. "github.com/pendo-io/statstash"
	"github.com/stretchr/testify/mock"
	"time"
)

type MockStatImplementation struct {
	mock.Mock
}

// MockStatImplementation is a no-op interface for testing
func (m *MockStatImplementation) IncrementCounter(name, source string) error {
	rargs := m.Called(name, source)
	return rargs.Error(0)
}

func (m *MockStatImplementation) IncrementCounterBy(name, source string, delta int64) error {
	rargs := m.Called(name, source, delta)
	return rargs.Error(0)
}

func (m *MockStatImplementation) RecordGauge(name, source string, value float64) error {
	rargs := m.Called(name, source, value)
	return rargs.Error(0)
}

func (m *MockStatImplementation) RecordTiming(name, source string, value, sampleRate float64) error {
	rargs := m.Called(name, source, value, sampleRate)
	return rargs.Error(0)
}

func (m *MockStatImplementation) UpdateBackend(at time.Time, flusher StatsFlusher, cfg *FlusherConfig, force bool) error {
	rargs := m.Called(at, flusher, cfg, force)
	return rargs.Error(0)
}
