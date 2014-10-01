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
)

func (s *StatStashTest) TestStatCounters(c *C) {

	ssi := StatInterfaceImplementation{s.Context}

	c.Assert(ssi.IncrementCounter("foo", "a"), IsNil)
	c.Assert(ssi.IncrementCounter("foo", "a"), IsNil)
	c.Assert(ssi.IncrementCounter("foo", "b"), IsNil)
	c.Assert(ssi.IncrementCounter("bar", ""), IsNil)
	c.Assert(ssi.IncrementCounter("bar", ""), IsNil)
	c.Assert(ssi.IncrementCounterBy("bar", "", int64(10)), IsNil)

	// at this point
	// foo, a = 2
	fooA, err := ssi.peekCounter("foo", "a")
	c.Assert(err, IsNil)
	c.Check(fooA, Equals, uint64(2))

	// foo, b = 1
	fooB, err := ssi.peekCounter("foo", "b")
	c.Assert(err, IsNil)
	c.Check(fooB, Equals, uint64(1))

	// bar = 1
	bar, err := ssi.peekCounter("bar", "")
	c.Assert(err, IsNil)
	c.Check(bar, Equals, uint64(12))

}
