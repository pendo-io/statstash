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
	"bytes"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/urlfetch"
	"io/ioutil"
	"net/http"
	"net/url"
)

const (
	libratoApiEndpoint = "https://metrics-api.librato.com/v1/metrics"
)

// LibratoStatsFlusher is used to flush stats to the Librato metrics service.
type LibratoStatsFlusher struct {
	c context.Context
}

func NewLibratoStatsFlusher(c context.Context) StatsFlusher {
	return LibratoStatsFlusher{c}
}

func (lf LibratoStatsFlusher) Flush(data []interface{}, cfg *FlusherConfig) error {

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
		case StatDataGauge:
			sdg := data[i].(StatDataGauge)
			postdata.Add(getPostKey("gauges", "name", gaugeCount), sdg.Name)
			postdata.Add(getPostKey("gauges", "value", gaugeCount), fmt.Sprintf("%f", sdg.Value))
			if sdg.Source != "" {
				postdata.Add(getPostKey("gauges", "source", gaugeCount), sdg.Source)
			}
			gaugeCount++
		case StatDataTiming:
			sdt := data[i].(StatDataTiming)
			postdata.Add(getPostKey("gauges", "name", gaugeCount), sdt.Name)
			postdata.Add(getPostKey("gauges", "count", gaugeCount), fmt.Sprintf("%d", sdt.Count))
			postdata.Add(getPostKey("gauges", "min", gaugeCount), fmt.Sprintf("%f", sdt.Min))
			postdata.Add(getPostKey("gauges", "max", gaugeCount), fmt.Sprintf("%f", sdt.Max))
			postdata.Add(getPostKey("gauges", "sum", gaugeCount), fmt.Sprintf("%f", sdt.Sum))
			postdata.Add(getPostKey("gauges", "sum_squares", gaugeCount), fmt.Sprintf("%f", sdt.SumSquares))
			if sdt.Source != "" {
				postdata.Add(getPostKey("gauges", "source", gaugeCount), sdt.Source)
			}
			gaugeCount++
			// Send a 90th percentile (9th decile) metric, too
			postdata.Add(getPostKey("gauges", "name", gaugeCount), sdt.Name+".90")
			postdata.Add(getPostKey("gauges", "count", gaugeCount), fmt.Sprintf("%d", sdt.NinthDecileCount))
			postdata.Add(getPostKey("gauges", "max", gaugeCount), fmt.Sprintf("%f", sdt.NinthDecileValue))
			postdata.Add(getPostKey("gauges", "sum", gaugeCount), fmt.Sprintf("%f", sdt.NinthDecileSum))
			if sdt.Source != "" {
				postdata.Add(getPostKey("gauges", "source", gaugeCount), sdt.Source)
			}
			gaugeCount++
		}
	}

	log.Debugf(lf.c, "Flushing data to Librato: %#v", postdata)

	req, _ := http.NewRequest("POST", libratoApiEndpoint, bytes.NewBuffer([]byte(postdata.Encode())))
	req.SetBasicAuth(cfg.Username, cfg.Password)
	if resp, err := lf.getHttpClient().Do(req); err != nil {
		log.Errorf(lf.c, "Failed to flush events to Librato: HTTP error: %s", err.Error())
		return err
	} else if resp.StatusCode != 200 && resp.StatusCode != 204 {
		defer resp.Body.Close()
		if body, err := ioutil.ReadAll(resp.Body); err != nil {
			log.Errorf(lf.c, "Failed to flush events to Librato, and failed to read the response body: %s", err)
		} else {
			log.Errorf(lf.c, "Failed to flush events to Librato: HTTP status code %d, response body: %s", resp.StatusCode, body)
		}
	}

	return nil
}

func (lf LibratoStatsFlusher) getHttpClient() *http.Client {
	return urlfetch.Client(lf.c)
}
