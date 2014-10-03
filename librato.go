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
	"appengine"
	"appengine/urlfetch"
	"bytes"
	"fmt"
	"net/http"
	"net/url"
)

const (
	libratoApiEndpoint = "https://metrics-api.librato.com/v1/metrics"
)

// LibratoStatsFlusher is used to flush stats to the Librato metrics service.
type LibratoStatsFlusher struct {
	c appengine.Context
}

func (lf LibratoStatsFlusher) Flush(data []interface{}, cfg FlusherConfig) error {

	postdata := make(url.Values)

	getPostKey := func(typ, field string, i int) string {
		return fmt.Sprintf("%s[%d][%s]", typ, i, field)
	}

	gaugeCount := 0
	counterCount := 0

	for i := range data {
		switch data[i].(type) {
		case StatDataCounter:
			sdc := data[i].(StatDataCounter)
			postdata.Add(getPostKey("counters", "name", counterCount), sdc.Name)
			postdata.Add(getPostKey("counters", "value", counterCount), fmt.Sprintf("%d", sdc.Count))
			if sdc.Source != "" {
				postdata.Add(getPostKey("counters", "source", counterCount), sdc.Source)
			}
			counterCount++
		case StatDataTiming:
			sdt := data[i].(StatDataTiming)
			postdata.Add(getPostKey("gauges", "name", gaugeCount), sdt.Name)
			// postdata.Add(getPostKey("gauges", "value", gaugeCount), fmt.Sprintf("%f", sdt.Va))
			if sdt.Source != "" {
				postdata.Add(getPostKey("gauges", "source", counterCount), sdt.Source)
			}
			gaugeCount++
		}
	}

	lf.c.Debugf("Flushing data to Librato: %#v", postdata)

	req, _ := http.NewRequest("POST", libratoApiEndpoint, bytes.NewBuffer([]byte(postdata.Encode())))
	req.SetBasicAuth(cfg.Username, cfg.Password)
	if _, err := lf.getHttpClient().Do(req); err != nil {
		lf.c.Errorf("failed to log events to librato: %s", err.Error())
		return err
	}

	return nil
}

func (lf LibratoStatsFlusher) getHttpClient() *http.Client {
	return urlfetch.Client(lf.c)
}