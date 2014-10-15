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
	"math"
	"math/rand"
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

	ssi := StatImplementation{s.Context, rand.New(rand.NewSource(time.Now().UnixNano())), true}

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

	ssi := StatImplementation{s.Context, rand.New(rand.NewSource(time.Now().UnixNano())), true}

	c.Assert(ssi.RecordGauge("TestStatGauge.subroutine", "A", 24.0), IsNil)
	c.Assert(ssi.RecordGauge("TestStatGauge.subroutine", "B", 10.0), IsNil)
	c.Assert(ssi.RecordGauge("TestStatGauge.subroutine", "B", 15.5), IsNil)
	c.Assert(ssi.RecordGauge("TestStatGauge.grand_total", "", 7264534001), IsNil)

	now := time.Now()

	subA, err := ssi.peekGauge("TestStatGauge.subroutine", "A", now)
	c.Assert(err, IsNil)
	c.Assert(subA, HasLen, 1)
	c.Check(subA[0], Equals, 24.0)

	subB, err := ssi.peekGauge("TestStatGauge.subroutine", "B", now)
	c.Assert(err, IsNil)
	c.Assert(subB, HasLen, 1)
	c.Check(subB[0], Equals, 15.5)

	grand, err := ssi.peekGauge("TestStatGauge.grand_total", "", now)
	c.Assert(err, IsNil)
	c.Assert(grand, HasLen, 1)
	c.Check(grand[0], Equals, float64(7264534001))

	for i := 0; i < 10; i++ {
		c.Assert(ssi.RecordGauge("TestStatGauge.upandtotheright", "", float64(i)), IsNil)
	}

	upAndToTheRight, err := ssi.peekGauge("TestStatGauge.upandtotheright", "", now)
	c.Assert(err, IsNil)
	c.Assert(upAndToTheRight, HasLen, 1)
	c.Check(upAndToTheRight[0], Equals, float64(9))

}

func (s *StatStashTest) TestStatTimings(c *C) {

	ssi := StatImplementation{s.Context, rand.New(rand.NewSource(time.Now().UnixNano())), true}

	c.Assert(ssi.RecordTiming("TestStatTimings.subroutine", "A", 24.0, 1.0), IsNil)
	c.Assert(ssi.RecordTiming("TestStatTimings.subroutine", "B", 10.0, 1.0), IsNil)
	c.Assert(ssi.RecordTiming("TestStatTimings.subroutine", "B", 15.5, 1.0), IsNil)
	c.Assert(ssi.RecordTiming("TestStatTimings.grand_total", "", 7264534001.0, 1.0), IsNil)

	now := time.Now()

	subA, err := ssi.peekTiming("TestStatTimings.subroutine", "A", now)
	c.Assert(err, IsNil)
	c.Assert(subA, HasLen, 1)
	c.Check(subA[0], Equals, 24.0)

	subB, err := ssi.peekTiming("TestStatTimings.subroutine", "B", now)
	c.Assert(err, IsNil)
	c.Assert(subB, HasLen, 2)
	c.Check(subB[0], Equals, 10.0)
	c.Check(subB[1], Equals, 15.5)

	grand, err := ssi.peekTiming("TestStatTimings.grand_total", "", now)
	c.Assert(err, IsNil)
	c.Assert(grand, HasLen, 1)
	c.Check(grand[0], Equals, 7264534001.0)

	for i := 0; i < 10; i++ {
		c.Assert(ssi.RecordTiming("TestStatTimings.upandtotheright", "", float64(i), 1.0), IsNil)
	}

	upAndToTheRight, err := ssi.peekTiming("TestStatTimings.upandtotheright", "", now)
	c.Assert(err, IsNil)
	for i, metric := range upAndToTheRight {
		c.Check(metric, Equals, float64(i))
	}

}

func (s *StatStashTest) TestGetActiveConfigs(c *C) {

	ssi := StatImplementation{s.Context, rand.New(rand.NewSource(time.Now().UnixNano())), true}

	c.Assert(ssi.Purge(), IsNil)

	c.Assert(ssi.IncrementCounter("TestGetActiveConfigs.foo", "a"), IsNil)
	c.Assert(ssi.IncrementCounter("TestGetActiveConfigs.foo", "a"), IsNil)
	c.Assert(ssi.IncrementCounter("TestGetActiveConfigs.foo", "b"), IsNil)
	c.Assert(ssi.IncrementCounter("TestGetActiveConfigs.bar", ""), IsNil)
	c.Assert(ssi.IncrementCounter("TestGetActiveConfigs.bar", ""), IsNil)
	c.Assert(ssi.IncrementCounterBy("TestGetActiveConfigs.bar", "", int64(10)), IsNil)

	now := time.Now()
	bucketTs := getStartOfFlushPeriod(now, 0).Unix()

	cfgMap, err := ssi.getActiveConfigs(now, 0)
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

	ssi := StatImplementation{s.Context, rand.New(rand.NewSource(time.Now().UnixNano())), true}

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

	c.Assert(ssi.RecordTiming("TestFlushToBackend.subroutine", "A", 24.0, 1.0), IsNil)
	c.Assert(ssi.RecordTiming("TestFlushToBackend.subroutine", "B", 10.0, 1.0), IsNil)
	c.Assert(ssi.RecordTiming("TestFlushToBackend.subroutine", "B", 15.5, 1.0), IsNil)

	for i := 0; i < 10; i++ {
		c.Assert(ssi.RecordTiming("TestFlushToBackend.upandtotheright", "", float64(i), 1.0), IsNil)
	}

	mockFlusher.On("Flush", mock.Anything, mock.Anything).Return(nil).Once()

	now := time.Now()
	c.Assert(ssi.UpdateBackend(now, mockFlusher, nil, true), IsNil)
	mockFlusher.AssertExpectations(c)

	c.Check(mockFlusher.counters, HasLen, 3)
	c.Check(mockFlusher.timings, HasLen, 3)
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
			c.Check(timing.Median, Equals, 24.0)
			c.Check(timing.NinthDecileCount, Equals, 1)
			c.Check(timing.NinthDecileValue, Equals, 24.0)
			c.Check(timing.NinthDecileSum, Equals, 24.0)
		case "TestFlushToBackend.subroutine-B":
			c.Check(timing.Count, Equals, 2)
			c.Check(timing.Min, Equals, 10.0)
			c.Check(timing.Max, Equals, 15.5)
			c.Check(timing.Sum, Equals, 25.5)
			c.Check(timing.SumSquares, Equals, 340.25)
			c.Check(timing.Median, Equals, 12.75)
			c.Check(timing.NinthDecileCount, Equals, 2)
			c.Check(timing.NinthDecileValue, Equals, 15.5)
			c.Check(timing.NinthDecileSum, Equals, 25.5)
		case "TestFlushToBackend.upandtotheright-":
			c.Check(timing.Count, Equals, 10)
			c.Check(timing.Min, Equals, 0.0)
			c.Check(timing.Max, Equals, 9.0)
			c.Check(timing.Sum, Equals, 45.0)
			c.Check(timing.SumSquares, Equals, 285.0)
			c.Check(timing.Median, Equals, 4.5)
			c.Check(timing.NinthDecileCount, Equals, 9)
			c.Check(timing.NinthDecileValue, Equals, 8.0)
			c.Check(timing.NinthDecileSum, Equals, 36.0)
		}
	}

	for _, gauge := range mockFlusher.gauges {
		nameAndSource := fmt.Sprintf("%s-%s", gauge.Name, gauge.Source)
		switch nameAndSource {
		case "TestFlushToBackend.foo-a":
			c.Check(gauge, Equals, 24.0)
		case "TestFlushToBackend.foo-b":
			c.Check(gauge, Equals, 15.5)
		case "TestFlushToBackend.bar-":
			c.Check(gauge, Equals, -1.2)
		}
	}

	// Make sure that flushing again does nothing (i.e. don't force)
	c.Assert(ssi.UpdateBackend(now, mockFlusher, nil, false), Equals, ErrStatFlushTooSoon)

}

func (s *StatStashTest) TestPeriodStart(c *C) {

	utc, _ := time.LoadLocation("UTC")
	ref := time.Date(2014, 10, 4, 12, 0, 0, 0, utc)

	c.Check(getStartOfFlushPeriod(ref, 0).Unix(), Equals, ref.Unix())
	c.Check(getStartOfFlushPeriod(ref.Add(1*time.Second), 0).Unix(), Equals, ref.Unix())

	c.Check(getStartOfFlushPeriod(ref, -1).Unix(), Equals, ref.Add(defaultAggregationPeriod*time.Duration(-1)).Unix())
	c.Check(getStartOfFlushPeriod(ref.Add(1*time.Second), -1).Unix(), Equals, ref.Add(defaultAggregationPeriod*time.Duration(-1)).Unix())

}

type StatSamplingTestImplementation struct {
	randGen *rand.Rand
}

func (c StatSamplingTestImplementation) IncrementCounter(name, source string) error { return nil }
func (c StatSamplingTestImplementation) IncrementCounterBy(name, source string, delta int64) error {
	return nil
}
func (c StatSamplingTestImplementation) RecordGauge(name, source string, value float64) error {
	return nil
}
func (c StatSamplingTestImplementation) RecordTiming(name, source string, value, sampleRate float64) error {

	// We use this code copied from the other code to prevent actually having to
	// use memcache and blowing up the test suite.
	if sampleRate < 1.0 && c.randGen.Float64() > sampleRate {
		return ErrStatNotSampled // do nothing here, as we are sampling
	}
	return nil
}
func (c StatSamplingTestImplementation) UpdateBackend(periodStart time.Time, flusher StatsFlusher, cfg *FlusherConfig, force bool) error {
	return nil
}

func (s *StatStashTest) TestTimingSampling(c *C) {
	ssi := StatSamplingTestImplementation{rand.New(rand.NewSource(time.Now().UnixNano()))}

	// Let's record a million timings at a sample rate of 0.0001.
	// We'll expect 100 samples, give or take 50
	statsSampled := 0
	for i := 0; i < 1000000; i++ {
		if err := ssi.RecordTiming("yowza", "fast", 1, 0.0001); err == ErrStatNotSampled {
			continue
		} else if err != nil {
			// unexpected error, fail
			c.Fail()
		} else {
			statsSampled++
		}
	}
	fmt.Printf("Stats sampled %d\n", statsSampled)
	c.Assert(math.Abs(100.0-float64(statsSampled)) <= 50.0, Equals, true)

}
