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
	"appengine"
	"net/http"
	"time"
)

func PeriodicStatsFlushHandler(flusher StatsFlusher, cfg *FlusherConfig, r *http.Request) {
	c := appengine.NewContext(r)
	stats := NewStatInterface(c, false)
	doFlush(c, stats, flusher, cfg)
}

func PeriodicStatsFlushHandlerCustom(c appengine.Context, stats StatInterface, flusher StatsFlusher, cfg *FlusherConfig) {
	doFlush(c, stats, flusher, cfg)
}

func doFlush(c appengine.Context, stats StatInterface, flusher StatsFlusher, cfg *FlusherConfig) {
	startOfLastPeriod := getStartOfFlushPeriod(time.Now(), -1)
	if err := stats.UpdateBackend(startOfLastPeriod, flusher, cfg, false); err != nil {
		c.Errorf("Failed updating stats backend: %s", err)
	} else {
		c.Infof("Updated stats backend")
	}
}
