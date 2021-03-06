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

package statstash

import (
	"github.com/pendo-io/appwrap"
	"golang.org/x/net/context"
	"gopkg.in/check.v1"
	"testing"
)

type StatStashTest struct {
	Context context.Context
}

var _ = check.Suite(&StatStashTest{})

func TestStatStash(t *testing.T) { check.TestingT(t) }

func (s *StatStashTest) SetUpSuite(c *check.C) {
	s.Context = appwrap.StubContext()
}
