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
	"net/http"
	"time"

	"github.com/pendo-io/appwrap"
	"google.golang.org/appengine"
)

func PeriodicStatsFlushHandler(flusher StatsFlusher, cfg *FlusherConfig, r *http.Request) {
	c := appengine.NewContext(r)
	log := appwrap.NewAppengineLogging(c)
	ds, err := appwrap.NewDatastore(c)
	if err != nil {
		panic(err)
	}
	stats := NewStatInterface(log, ds, appwrap.NewAppengineMemcache(c, "", "", 0), false)
	doFlush(log, stats, flusher, cfg)
}

func PeriodicStatsFlushHandlerCustom(log appwrap.Logging, stats StatInterface, flusher StatsFlusher, cfg *FlusherConfig) {
	doFlush(log, stats, flusher, cfg)
}

func doFlush(log appwrap.Logging, stats StatInterface, flusher StatsFlusher, cfg *FlusherConfig) {
	startOfLastPeriod := getStartOfFlushPeriod(time.Now(), -1)
	if err := stats.UpdateBackend(startOfLastPeriod, flusher, cfg, false); err != nil {
		log.Errorf("Failed updating stats backend: %s", err)
	} else {
		log.Infof("Updated stats backend")
	}
}
