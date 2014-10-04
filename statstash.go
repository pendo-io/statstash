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
	"appengine/datastore"
	"appengine/memcache"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

const (
	dsKindStatConfig         = "StatConfig"
	scTypeTiming             = "timing"
	scTypeGauge              = "gauge"
	scTypeCounter            = "counter"
	defaultAggregationPeriod = time.Duration(5 * time.Minute)
)

var ErrStatFlushTooSoon = errors.New("Too Soon to Flush Stats")

type StatConfig struct {
	Name     string    `datastore:",noindex" json:"name"`
	Source   string    `datastore:",noindex" json:"source"`
	Type     string    `datastore:",noindex" json:"type"`
	LastRead time.Time `json:"lastread"`
}

func (sc StatConfig) String() string {
	return fmt.Sprintf("[StatConfig] name=%s, source=%s, type=%s, lastread=%s",
		sc.Name, sc.Source, sc.Type, sc.LastRead)
}

func (sc StatConfig) BucketKey(t time.Time, offset int) string {
	return fmt.Sprintf("ss-metric:%s-%s-%s-%d", sc.Type, sc.Name, sc.Source, getFlushPeriodStart(t, offset).Unix())
}

// StatInterface defines the interface for the application to
type StatInterface interface {
	IncrementCounter(name, source string) error
	IncrementCounterBy(name, source string, delta int64) error
	RecordGauge(name, source string, value float64) error
	RecordTiming(name, source string, value float64) error
	UpdateBackend(at time.Time, flusher StatsFlusher, cfg *FlusherConfig, force bool) error
}

func NewLoggingStatImplementation(c appengine.Context) StatInterface {
	return LoggingStatImplementation{c}
}

type LoggingStatImplementation struct {
	c appengine.Context
}

// LoggingStatImplementation is a no-op interface for testing; logs to debug level only
func (m LoggingStatImplementation) IncrementCounter(name, source string) error {
	m.c.Debugf("IncrementCounter(%s, %s)", name, source)
	return nil
}

func (m LoggingStatImplementation) IncrementCounterBy(name, source string, delta int64) error {
	m.c.Debugf("IncrementCounterBy(%s, %s, %d)", name, source, delta)
	return nil
}

func (m LoggingStatImplementation) RecordGauge(name, source string, value float64) error {
	m.c.Debugf("RecordGauge(%s, %s, %f)", name, source, value)
	return nil
}

func (m LoggingStatImplementation) RecordTiming(name, source string, value float64) error {
	m.c.Debugf("RecordTiming(%s, %s, %f)", name, source, value)
	return nil
}

func (m LoggingStatImplementation) UpdateBackend(at time.Time, flusher StatsFlusher, cfg *FlusherConfig, force bool) error {
	m.c.Debugf("UpdateBackend(%s, %#v, %#v, %t)", at, flusher, cfg, force)
	return nil
}

func NewStatInterface(c appengine.Context) StatInterface {
	return StatImplementation{c}
}

type StatImplementation struct {
	c appengine.Context
}

func (s StatImplementation) IncrementCounter(name, source string) error {
	return s.IncrementCounterBy(name, source, 1)
}

func (s StatImplementation) IncrementCounterBy(name, source string, delta int64) error {

	bucketKey, err := s.getBucketKey(scTypeCounter, name, source, time.Now())
	if err != nil {
		return err
	}

	if _, err = memcache.Increment(s.c, bucketKey, delta, 0); err != nil {
		s.c.Warningf("Failed to increment %s delta %d", bucketKey, delta)
	}

	return err
}

func (s StatImplementation) RecordGauge(name, source string, value float64) error {
	return s.recordGaugeOrTiming(scTypeGauge, name, source, value)
}

func (s StatImplementation) RecordTiming(name, source string, value float64) error {
	return s.recordGaugeOrTiming(scTypeTiming, name, source, value)
}

func (s StatImplementation) UpdateBackend(at time.Time, flusher StatsFlusher, flushConfig *FlusherConfig, force bool) error {

	if !force {
		lastFlushedTime := s.getLastFlushTime()
		if at.Sub(lastFlushedTime) <= defaultAggregationPeriod {
			s.c.Warningf("Refusing to update backend since it's too soon (last flushed at %s, aggregation period %s)", lastFlushedTime, defaultAggregationPeriod)
			return ErrStatFlushTooSoon
		}
	}

	cfgMap, err := s.getActiveConfigs(at, 0)
	if err != nil {
		s.c.Errorf("Failed to get active buckets when updating backend: %s", err)
		return err
	}

	if len(cfgMap) == 0 {
		return nil // nothing to do
	}

	bucketKeys := make([]string, len(cfgMap))
	for k := range cfgMap {
		bucketKeys = append(bucketKeys, k)
	}

	if itemMap, err := memcache.GetMulti(s.c, bucketKeys); err != nil {
		s.c.Errorf("Failed to fetch items from memcache when updating backend: %s", err)
	} else {

		// Get our data from memcache in one go
		data := make([]interface{}, 0, len(itemMap))
		for k, item := range itemMap {
			var datum interface{}
			cfgItem := cfgMap[k]
			switch cfgItem.Type {
			case scTypeTiming, scTypeGauge:
				var gm []Measurement
				if err := memcache.Gob.Unmarshal(item.Value, &gm); err != nil {
					s.c.Errorf("Bad data found in memcache: key %s, error: %s", k, err)
					continue
				}
				if cfgItem.Type == scTypeTiming {
					var min, max, sum, sumSquares float64
					count := len(gm)
					for i, m := range gm {
						if i == 0 {
							min = m.Value
							max = m.Value
						} else {
							if m.Value < min {
								min = m.Value
							}
							if m.Value > max {
								max = m.Value
							}
						}
						sum += m.Value
						sumSquares += math.Pow(m.Value, 2.0)
					}
					datum = StatDataTiming{StatConfig: cfgItem, Count: count, Min: min, Max: max, Sum: sum, SumSquares: sumSquares}
				} else {
					datum = StatDataGauge{StatConfig: cfgItem, Value: gm[0].Value}
				}
			case scTypeCounter:
				count, _ := strconv.ParseUint(string(item.Value), 10, 64)
				datum = StatDataCounter{StatConfig: cfgItem, Count: count}
			default:
				panic("If this happened, things are horribly wrong.")
			}
			data = append(data, datum)
		}

		// Now flush to the backend
		if err := flusher.Flush(data, flushConfig); err != nil {
			s.c.Errorf("Failed to flush to backend: %s", err)
			return err
		} else {
			s.updateLastFlushTime(at)
		}

	}

	return nil

}

func (s StatImplementation) Purge() error {

	sc, err := s.getAllConfigs()
	if err != nil {
		return err
	}
	if len(sc) == 0 {
		return nil // nothing to do
	}

	now := time.Now()
	dsKeys := make([]*datastore.Key, 0, len(sc))
	memcacheKeys := make([]string, 0, len(sc))
	for _, cfg := range sc {
		dsKeys = append(dsKeys, s.getStatConfigDatastoreKey(cfg.Type, cfg.Name, cfg.Source))
		memcacheKeys = append(memcacheKeys, cfg.BucketKey(now, 0))
		memcacheKeys = append(memcacheKeys, cfg.BucketKey(now, -1))

	}

	if err := datastore.DeleteMulti(s.c, dsKeys); err != nil {
		s.c.Errorf("Stats: purge datastore failed: %s", err)
		return err
	}

	memcache.DeleteMulti(s.c, memcacheKeys)
	return nil
}

func (s StatImplementation) getAllConfigs() ([]StatConfig, error) {
	q := datastore.NewQuery(dsKindStatConfig)
	var cfgs []StatConfig
	_, err := q.GetAll(s.c, &cfgs)
	return cfgs, err
}

func (s StatImplementation) getActiveConfigs(at time.Time, offset int) (map[string]StatConfig, error) {

	statConfigs := make(map[string]StatConfig)

	var finalError error
	cutoffTime := at.Add(time.Duration(time.Hour * 24 * -2))
	s.c.Debugf("Looking for last read %s", cutoffTime)

	q := datastore.NewQuery(dsKindStatConfig).Filter("LastRead >", cutoffTime)
	iter := q.Run(s.c)
	for {
		var sc StatConfig
		_, err := iter.Next(&sc)
		if err == datastore.Done {
			break // end of iteration
		} else if err != nil {
			s.c.Warningf("Failed iterating stat config items to get active buckets: %s", err)
			finalError = err
			break
		}
		bucketKey := sc.BucketKey(at, offset)
		s.c.Debugf("Found %s, bucketKey %s", sc, bucketKey)

		statConfigs[bucketKey] = sc

	}
	return statConfigs, finalError
}

func (s StatImplementation) getBucketKey(typ, name, source string, at time.Time) (string, error) {
	statConfig, err := s.getStatConfig(typ, name, source)
	if err != nil {
		return "", err
	}

	return statConfig.BucketKey(at, 0), nil
}

func (s StatImplementation) getStatConfigKeyName(typ, name, source string) string {
	return fmt.Sprintf("%s-%s-%s", typ, name, source)
}

func (s StatImplementation) getStatConfigMemcacheKey(typ, name, source string) string {
	return fmt.Sprintf("ss-conf:%s", s.getStatConfigKeyName(typ, name, source))
}

func (s StatImplementation) getStatConfigDatastoreKey(typ, name, source string) *datastore.Key {
	return datastore.NewKey(s.c, dsKindStatConfig, s.getStatConfigKeyName(typ, name, source), 0, nil)
}

func (s StatImplementation) getStatConfig(typ, name, source string) (StatConfig, error) {

	var sc StatConfig

	// First, query memcache
	if _, err := memcache.Gob.Get(s.c, s.getStatConfigMemcacheKey(typ, name, source), &sc); err == nil {
		return sc, nil
	}

	k := s.getStatConfigDatastoreKey(typ, name, source)
	now := time.Now()
	updateNeeded := false
	cache := true

	// Now query datastore
	if err := datastore.Get(s.c, k, &sc); err != nil && err != datastore.ErrNoSuchEntity {
		return StatConfig{}, err
	} else if err == datastore.ErrNoSuchEntity {
		sc.Name = name
		sc.Source = source
		sc.Type = typ
		sc.LastRead = now
		updateNeeded = true
	} else {
		if now.Sub(sc.LastRead) >= time.Duration(2*24*time.Hour) {
			sc.LastRead = now
			updateNeeded = true
		}
	}

	// Store item in datastore if it needed the update
	if updateNeeded {
		if _, err := datastore.Put(s.c, k, &sc); err != nil {
			s.c.Warningf("Failed to update StatConfig %s: %s", sc, err)
			cache = false
		}
	}

	// Only attempt adding if the update was needed and succeeded
	if cache {
		memcache.Gob.Add(s.c, &memcache.Item{
			Key:        s.getStatConfigMemcacheKey(typ, name, source),
			Object:     &sc,
			Expiration: time.Duration(24 * time.Hour),
		})
	}

	return sc, nil

}

func (s StatImplementation) peekCounter(name, source string, at time.Time) (uint64, error) {

	bucketKey, err := s.getBucketKey(scTypeCounter, name, source, time.Now())
	if err != nil {
		return uint64(0), err
	}

	if item, err := memcache.Get(s.c, bucketKey); err == nil {
		return strconv.ParseUint(string(item.Value), 10, 64)
	} else {
		return uint64(0), err
	}
}

func (s StatImplementation) peekGauge(name, source string, at time.Time) ([]Measurement, error) {

	bucketKey, err := s.getBucketKey(scTypeGauge, name, source, time.Now())
	if err != nil {
		return nil, err
	}

	var gm []Measurement
	if _, err = memcache.Gob.Get(s.c, bucketKey, &gm); err != nil {
		return nil, err
	} else {
		return gm, nil
	}
}

func (s StatImplementation) peekTiming(name, source string, at time.Time) ([]Measurement, error) {

	bucketKey, err := s.getBucketKey(scTypeTiming, name, source, time.Now())
	if err != nil {
		return nil, err
	}

	var gm []Measurement
	if _, err = memcache.Gob.Get(s.c, bucketKey, &gm); err != nil {
		return nil, err
	} else {
		return gm, nil
	}
}

func (s StatImplementation) recordGaugeOrTiming(typ, name, source string, value float64) error {

	now := time.Now()
	bucketKey, err := s.getBucketKey(typ, name, source, now)
	if err != nil {
		return err
	}

	var cached []Measurement

	for tries := 0; tries < 5; tries++ {
		cachedItem, err := memcache.Gob.Get(s.c, bucketKey, &cached)
		if err == memcache.ErrCacheMiss {
			cached = make([]Measurement, 0)
		} else if err != nil {
			// give up fast because something is wrong with memcache, probably
			return err
		}

		switch typ {
		case scTypeTiming:
			cached = append(cached, Measurement{Timestamp: now.Unix(), Value: value})
		case scTypeGauge:
			cached = []Measurement{Measurement{Timestamp: now.Unix(), Value: value}}

		}

		if cachedItem != nil {
			cachedItem.Object = &cached
			if err := memcache.Gob.CompareAndSwap(s.c, cachedItem); err == nil {
				return nil
			} else if err == memcache.ErrCASConflict {
				time.Sleep(time.Microsecond * 50)
				continue // do it again from the top
			} else if err == memcache.ErrNotStored {
				if err := memcache.Gob.Add(s.c, cachedItem); err == memcache.ErrNotStored {
					return nil
				}
			} else {
				return err // something went horribly wrong, bail
			}
		}

		// If we made it this far, we'll need to just make a new item and store it
		newItem := &memcache.Item{
			Key:        bucketKey,
			Object:     &cached,
			Expiration: time.Duration(2 * defaultAggregationPeriod),
		}
		if err := memcache.Gob.Add(s.c, newItem); err == nil {
			return nil
		} else if err == memcache.ErrNotStored {
			// someone has jumped in front of us
			time.Sleep(time.Microsecond * 500)
			continue
		}
		return err
	}

	s.c.Errorf("Failed to store/update gauge for bucket %s: %s", bucketKey, err)
	return fmt.Errorf("Failed to store/update gauge for bucket %s; too many failures.")

}

func (s StatImplementation) getLastFlushTime() time.Time {
	var lastUpdated time.Time
	if _, err := memcache.Gob.Get(s.c, "ss-lft", &lastUpdated); err != nil {
		return time.Time{}
	}
	return lastUpdated
}

func (s StatImplementation) updateLastFlushTime(flushTime time.Time) error {
	return memcache.Gob.Set(s.c, &memcache.Item{
		Key:    "ss-lft",
		Object: &flushTime,
	})
}

func getFlushPeriodStart(at time.Time, offset int) time.Time {
	startOfPeriod := at.Truncate(defaultAggregationPeriod)
	if offset != 0 {
		startOfPeriod = startOfPeriod.Add(time.Duration(offset) * defaultAggregationPeriod)
	}
	return startOfPeriod
}

type Measurement struct {
	Timestamp int64
	Value     float64
}

func (m Measurement) String() string {
	fmtString := "[TS: %d, Value: %f]"
	return fmt.Sprintf(fmtString, m.Timestamp, m.Value)
}

type StatDataCounter struct {
	StatConfig
	Count uint64
}

func (dc StatDataCounter) String() string {
	return fmt.Sprintf("[Counter: name=%s, source=%s] Value: %d",
		dc.Name, dc.Source, dc.Count)
}

type StatDataTiming struct {
	StatConfig
	Count      int
	Min        float64
	Max        float64
	Sum        float64
	SumSquares float64
}

func (dt StatDataTiming) String() string {
	return fmt.Sprintf("[Timing: name=%s, source=%s] Count: %d, Min: %f, Max: %f, Sum: %f, SumSquares: %f",
		dt.Name, dt.Source, dt.Count, dt.Min, dt.Max, dt.Sum, dt.SumSquares)
}

type StatDataGauge struct {
	StatConfig
	Value interface{}
}

func (dg StatDataGauge) String() string {
	return fmt.Sprintf("[Gauge: name=%s, source=%s] Value: %s",
		dg.Name, dg.Source, dg.Value)
}

// StatsFlusher is an interface used to flush stats to various locations
type StatsFlusher interface {
	Flush(data []interface{}, cfg *FlusherConfig) error
}

type FlusherConfig struct {
	Username string
	Password string
	ApiKey   string
}

// LogOnlyStatsFlusher is used to "flush" stats for testing and development.
// Stats that are flushed are logged only.
type LogOnlyStatsFlusher struct {
	c appengine.Context
}

func NewLogOnlyStatsFlusher(c appengine.Context) StatsFlusher {
	return LogOnlyStatsFlusher{c}
}

func (f LogOnlyStatsFlusher) Flush(data []interface{}, cfg *FlusherConfig) error {
	for i := range data {
		var datum interface{}
		switch data[i].(type) {
		case StatDataCounter:
			datum = data[i].(StatDataCounter)
		case StatDataTiming:
			datum = data[i].(StatDataTiming)
		case StatDataGauge:
			datum = data[i].(StatDataGauge)
		}
		f.c.Infof("%s", datum)
	}
	return nil
}
