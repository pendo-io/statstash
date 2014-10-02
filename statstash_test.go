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
	. "gopkg.in/check.v1"
	"time"
)

func (s *StatStashTest) TestStatCounters(c *C) {

	ssi := StatInterfaceImplementation{s.Context}

	c.Assert(ssi.IncrementCounter("foo", "a"), IsNil)
	c.Assert(ssi.IncrementCounter("foo", "a"), IsNil)
	c.Assert(ssi.IncrementCounter("foo", "b"), IsNil)
	c.Assert(ssi.IncrementCounter("bar", ""), IsNil)
	c.Assert(ssi.IncrementCounter("bar", ""), IsNil)
	c.Assert(ssi.IncrementCounterBy("bar", "", int64(10)), IsNil)

	now := time.Now()

	// at this point
	// foo, a = 2

	fooA, err := ssi.peekCounter("foo", "a", now)
	c.Assert(err, IsNil)
	c.Check(fooA, Equals, uint64(2))

	// foo, b = 1
	fooB, err := ssi.peekCounter("foo", "b", now)
	c.Assert(err, IsNil)
	c.Check(fooB, Equals, uint64(1))

	// bar = 1
	bar, err := ssi.peekCounter("bar", "", now)
	c.Assert(err, IsNil)
	c.Check(bar, Equals, uint64(12))

}

func (s *StatStashTest) TestStatGauges(c *C) {

	ssi := StatInterfaceImplementation{s.Context}

	c.Assert(ssi.RecordGauge("temperature", "raleigh", 24.0), IsNil)
	c.Assert(ssi.RecordGauge("temperature", "anchorage", 10.0), IsNil)
	c.Assert(ssi.RecordGauge("temperature", "anchorage", 15.5), IsNil)
	c.Assert(ssi.RecordGauge("world_population", "", 7264534001), IsNil)

	now := time.Now()

	tempRaleighMetrics, err := ssi.peekGauge("temperature", "raleigh", now)
	c.Assert(err, IsNil)
	c.Assert(tempRaleighMetrics, HasLen, 1)
	c.Check(tempRaleighMetrics[0].Value, Equals, 24.0)

	tempAnchorageMetrics, err := ssi.peekGauge("temperature", "anchorage", now)
	c.Assert(err, IsNil)
	c.Assert(tempAnchorageMetrics, HasLen, 2)
	c.Check(tempAnchorageMetrics[0].Value, Equals, 10.0)
	c.Check(tempAnchorageMetrics[1].Value, Equals, 15.5)

	worldPop, err := ssi.peekGauge("world_population", "", now)
	c.Assert(err, IsNil)
	c.Assert(worldPop, HasLen, 1)
	c.Check(worldPop[0].Value, Equals, float64(7264534001))

	for i := 0; i < 100; i++ {
		c.Assert(ssi.RecordGauge("upandtotheright", "", i), IsNil)
	}

	upAndToTheRight, err := ssi.peekGauge("upandtotheright", "", now)
	c.Assert(err, IsNil)
	for i, metric := range upAndToTheRight {
		c.Check(metric.Value, Equals, float64(i))
	}

}
