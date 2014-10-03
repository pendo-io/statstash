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

// Package statstash is a service used to collect statistics
// for a Google App Engine project and package them up to a backend server.
package statstash

import (
	"fmt"
	"github.com/stretchr/testify/mock"
	. "gopkg.in/check.v1"
	"time"
)

type MockFlusher struct {
	mock.Mock
	counters []StatDataCounter
	timings  []StatDataTiming
	gauges   []StatDataGauge
}

func (m *MockFlusher) Flush(data []interface{}, cfg *FlusherConfig) error {
	rargs := m.Called(data, cfg)
	m.counters = make([]StatDataCounter, 0)
	m.timings = make([]StatDataTiming, 0)
	m.gauges = make([]StatDataGauge, 0)
	for i := range data {
		switch data[i].(type) {
		case StatDataCounter:
			m.counters = append(m.counters, data[i].(StatDataCounter))
		case StatDataTiming:
			m.timings = append(m.timings, data[i].(StatDataTiming))
		case StatDataGauge:
			m.gauges = append(m.gauges, data[i].(StatDataGauge))
		}
	}
	return rargs.Error(0)
}

func (s *StatStashTest) TestStatCounters(c *C) {

	ssi := StatInterfaceImplementation{s.Context}

	c.Assert(ssi.IncrementCounter("TestStatCounters.foo", "a"), IsNil)
	c.Assert(ssi.IncrementCounter("TestStatCounters.foo", "a"), IsNil)
	c.Assert(ssi.IncrementCounter("TestStatCounters.foo", "b"), IsNil)
	c.Assert(ssi.IncrementCounter("TestStatCounters.bar", ""), IsNil)
	c.Assert(ssi.IncrementCounter("TestStatCounters.bar", ""), IsNil)
	c.Assert(ssi.IncrementCounterBy("TestStatCounters.bar", "", int64(10)), IsNil)

	now := time.Now()

	// at this point
	// foo, a = 2

	fooA, err := ssi.peekCounter("TestStatCounters.foo", "a", now)
	c.Assert(err, IsNil)
	c.Check(fooA, Equals, uint64(2))

	// foo, b = 1
	fooB, err := ssi.peekCounter("TestStatCounters.foo", "b", now)
	c.Assert(err, IsNil)
	c.Check(fooB, Equals, uint64(1))

	// bar = 1
	bar, err := ssi.peekCounter("TestStatCounters.bar", "", now)
	c.Assert(err, IsNil)
	c.Check(bar, Equals, uint64(12))

}

func (s *StatStashTest) TestStatGauge(c *C) {

	ssi := StatInterfaceImplementation{s.Context}

	c.Assert(ssi.RecordGauge("TestStatGauge.subroutine", "A", 24.0), IsNil)
	c.Assert(ssi.RecordGauge("TestStatGauge.subroutine", "B", 10.0), IsNil)
	c.Assert(ssi.RecordGauge("TestStatGauge.subroutine", "B", 15.5), IsNil)
	c.Assert(ssi.RecordGauge("TestStatGauge.grand_total", "", 7264534001), IsNil)

	now := time.Now()

	subA, err := ssi.peekGauge("TestStatGauge.subroutine", "A", now)
	c.Assert(err, IsNil)
	c.Assert(subA, HasLen, 1)
	c.Check(subA[0].Value, Equals, 24.0)

	subB, err := ssi.peekGauge("TestStatGauge.subroutine", "B", now)
	c.Assert(err, IsNil)
	c.Assert(subB, HasLen, 1)
	c.Check(subB[0].Value, Equals, 15.5)

	grand, err := ssi.peekGauge("TestStatGauge.grand_total", "", now)
	c.Assert(err, IsNil)
	c.Assert(grand, HasLen, 1)
	c.Check(grand[0].Value, Equals, float64(7264534001))

	for i := 0; i < 10; i++ {
		c.Assert(ssi.RecordGauge("TestStatGauge.upandtotheright", "", float64(i)), IsNil)
	}

	upAndToTheRight, err := ssi.peekGauge("TestStatGauge.upandtotheright", "", now)
	c.Assert(err, IsNil)
	c.Assert(upAndToTheRight, HasLen, 1)
	c.Check(upAndToTheRight[0].Value, Equals, float64(9))

}

func (s *StatStashTest) TestStatTimings(c *C) {

	ssi := StatInterfaceImplementation{s.Context}

	c.Assert(ssi.RecordTiming("TestStatTimings.subroutine", "A", 24.0), IsNil)
	c.Assert(ssi.RecordTiming("TestStatTimings.subroutine", "B", 10.0), IsNil)
	c.Assert(ssi.RecordTiming("TestStatTimings.subroutine", "B", 15.5), IsNil)
	c.Assert(ssi.RecordTiming("TestStatTimings.grand_total", "", 7264534001.0), IsNil)

	now := time.Now()

	subA, err := ssi.peekTiming("TestStatTimings.subroutine", "A", now)
	c.Assert(err, IsNil)
	c.Assert(subA, HasLen, 1)
	c.Check(subA[0].Value, Equals, 24.0)

	subB, err := ssi.peekTiming("TestStatTimings.subroutine", "B", now)
	c.Assert(err, IsNil)
	c.Assert(subB, HasLen, 2)
	c.Check(subB[0].Value, Equals, 10.0)
	c.Check(subB[1].Value, Equals, 15.5)

	grand, err := ssi.peekTiming("TestStatTimings.grand_total", "", now)
	c.Assert(err, IsNil)
	c.Assert(grand, HasLen, 1)
	c.Check(grand[0].Value, Equals, 7264534001.0)

	for i := 0; i < 10; i++ {
		c.Assert(ssi.RecordTiming("TestStatTimings.upandtotheright", "", float64(i)), IsNil)
	}

	upAndToTheRight, err := ssi.peekTiming("TestStatTimings.upandtotheright", "", now)
	c.Assert(err, IsNil)
	for i, metric := range upAndToTheRight {
		c.Check(metric.Value, Equals, float64(i))
	}

}

func (s *StatStashTest) TestGetActiveConfigs(c *C) {

	ssi := StatInterfaceImplementation{s.Context}

	c.Assert(ssi.Purge(), IsNil)

	c.Assert(ssi.IncrementCounter("TestGetActiveConfigs.foo", "a"), IsNil)
	c.Assert(ssi.IncrementCounter("TestGetActiveConfigs.foo", "a"), IsNil)
	c.Assert(ssi.IncrementCounter("TestGetActiveConfigs.foo", "b"), IsNil)
	c.Assert(ssi.IncrementCounter("TestGetActiveConfigs.bar", ""), IsNil)
	c.Assert(ssi.IncrementCounter("TestGetActiveConfigs.bar", ""), IsNil)
	c.Assert(ssi.IncrementCounterBy("TestGetActiveConfigs.bar", "", int64(10)), IsNil)

	now := time.Now()
	bucketTs := now.Truncate(time.Duration(defaultAggregationPeriod)).Unix()

	cfgMap, err := ssi.getActiveConfigs(now)
	c.Assert(err, IsNil)
	c.Assert(cfgMap, HasLen, 3)

	for _, key := range []string{
		fmt.Sprintf("ss-metric:counter-TestGetActiveConfigs.foo-a-%d", bucketTs),
		fmt.Sprintf("ss-metric:counter-TestGetActiveConfigs.foo-b-%d", bucketTs),
		fmt.Sprintf("ss-metric:counter-TestGetActiveConfigs.bar--%d", bucketTs)} {
		_, found := cfgMap[key]
		c.Check(found, Equals, true)
	}

}

func (s *StatStashTest) TestFlushToBackend(c *C) {

	ssi := StatInterfaceImplementation{s.Context}

	mockFlusher := &MockFlusher{}

	c.Assert(ssi.Purge(), IsNil)

	c.Assert(ssi.IncrementCounter("TestFlushToBackend.foo", "a"), IsNil)
	c.Assert(ssi.IncrementCounter("TestFlushToBackend.foo", "a"), IsNil)
	c.Assert(ssi.IncrementCounter("TestFlushToBackend.foo", "b"), IsNil)
	c.Assert(ssi.IncrementCounter("TestFlushToBackend.bar", ""), IsNil)
	c.Assert(ssi.IncrementCounter("TestFlushToBackend.bar", ""), IsNil)
	c.Assert(ssi.IncrementCounterBy("TestFlushToBackend.bar", "", int64(10)), IsNil)

	c.Assert(ssi.RecordGauge("TestFlushToBackend.temperature", "raleigh", 24.0), IsNil)
	c.Assert(ssi.RecordGauge("TestFlushToBackend.temperature", "anchorage", 10.0), IsNil)
	c.Assert(ssi.RecordGauge("TestFlushToBackend.temperature", "anchorage", 15.5), IsNil)
	c.Assert(ssi.RecordGauge("TestFlushToBackend.temperature", "buffalo", -1.2), IsNil)

	c.Assert(ssi.RecordTiming("TestFlushToBackend.subroutine", "A", 24.0), IsNil)
	c.Assert(ssi.RecordTiming("TestFlushToBackend.subroutine", "B", 10.0), IsNil)
	c.Assert(ssi.RecordTiming("TestFlushToBackend.subroutine", "B", 15.5), IsNil)

	mockFlusher.On("Flush", mock.Anything, mock.Anything).Return(nil).Once()

	now := time.Now()
	c.Assert(ssi.UpdateBackend(now, mockFlusher, nil), IsNil)
	mockFlusher.AssertExpectations(c)

	c.Check(mockFlusher.counters, HasLen, 3)
	c.Check(mockFlusher.timings, HasLen, 2)
	c.Check(mockFlusher.gauges, HasLen, 3)

	for _, counter := range mockFlusher.counters {
		nameAndSource := fmt.Sprintf("%s-%s", counter.Name, counter.Source)
		switch nameAndSource {
		case "TestFlushToBackend.foo-a":
			c.Check(counter.Count, Equals, uint64(2))
		case "TestFlushToBackend.foo-b":
			c.Check(counter.Count, Equals, uint64(1))
		case "TestFlushToBackend.bar-":
			c.Check(counter.Count, Equals, uint64(12))
		}
	}

	for _, timing := range mockFlusher.timings {
		nameAndSource := fmt.Sprintf("%s-%s", timing.Name, timing.Source)
		switch nameAndSource {
		case "TestFlushToBackend.subroutine-A":
			c.Check(timing.Count, Equals, 1)
			c.Check(timing.Min, Equals, 24.0)
			c.Check(timing.Max, Equals, 24.0)
			c.Check(timing.Sum, Equals, 24.0)
			c.Check(timing.SumSquares, Equals, 576.0)
		case "TestFlushToBackend.subroutine-B":
			c.Check(timing.Count, Equals, 2)
			c.Check(timing.Min, Equals, 10.0)
			c.Check(timing.Max, Equals, 15.5)
			c.Check(timing.Sum, Equals, 25.5)
			c.Check(timing.SumSquares, Equals, 340.25)
		}
	}

	for _, gauge := range mockFlusher.gauges {
		nameAndSource := fmt.Sprintf("%s-%s", gauge.Name, gauge.Source)
		switch nameAndSource {
		case "TestFlushToBackend.foo-a":
			c.Check(gauge.Value, Equals, 24.0)
		case "TestFlushToBackend.foo-b":
			c.Check(gauge.Value, Equals, 15.5)
		case "TestFlushToBackend.bar-":
			c.Check(gauge.Value, Equals, -1.2)
		}
	}

}
