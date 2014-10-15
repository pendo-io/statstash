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
	"math/rand"
	"sort"
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
var ErrStatNotSampled = errors.New("Skipped sample because sample rate given")

type ErrStatDropped struct {
	typ    string
	name   string
	source string
	t      time.Time
	value  float64
	err    error
}

func NewErrStatDropped(typ, name, source string, t time.Time, value float64, err error) error {
	return &ErrStatDropped{typ, name, source, t, value, err}
}

func (e *ErrStatDropped) Error() string {
	return fmt.Sprintf("Stat not stored: [type/name/source: %s/%s/%s, time: %s, value: %f]. Reason: %s",
		e.typ, e.name, e.source, e.t, e.value, e.err)
}

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
	return fmt.Sprintf("ss-metric:%s-%s-%s-%d", sc.Type, sc.Name, sc.Source, getStartOfFlushPeriod(t, offset).Unix())
}

// StatInterface defines the interface for the application to
type StatInterface interface {
	IncrementCounter(name, source string) error
	IncrementCounterBy(name, source string, delta int64) error
	RecordGauge(name, source string, value float64) error
	RecordTiming(name, source string, value, sampleRate float64) error
	UpdateBackend(periodStart time.Time, flusher StatsFlusher, cfg *FlusherConfig, force bool) error
}

func NewNullStatImplementation() StatInterface {
	return NullStatImplementation{}
}

type NullStatImplementation struct {
}

func (m NullStatImplementation) IncrementCounter(name, source string) error { return nil }
func (m NullStatImplementation) IncrementCounterBy(name, source string, delta int64) error {
	return nil
}
func (m NullStatImplementation) RecordGauge(name, source string, value float64) error { return nil }
func (m NullStatImplementation) RecordTiming(name, source string, value, sampleRate float64) error {
	return nil
}
func (m NullStatImplementation) UpdateBackend(periodStart time.Time, flusher StatsFlusher, cfg *FlusherConfig, force bool) error {
	return nil
}

func NewStatInterface(c appengine.Context, debug bool) StatInterface {
	return StatImplementation{c, rand.New(rand.NewSource(time.Now().UnixNano())), debug}
}

type StatImplementation struct {
	c       appengine.Context
	randGen *rand.Rand
	debug   bool
}

func (s StatImplementation) IncrementCounter(name, source string) error {
	return s.IncrementCounterBy(name, source, 1)
}

func (s StatImplementation) IncrementCounterBy(name, source string, delta int64) error {
	s.debugf("Increment counter/%s/%s: delta=%d", name, source, delta)
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
	return s.recordGaugeOrTiming(scTypeGauge, name, source, value, 1.0)
}

func (s StatImplementation) RecordTiming(name, source string, value, sampleRate float64) error {
	return s.recordGaugeOrTiming(scTypeTiming, name, source, value, sampleRate)
}

func (s StatImplementation) UpdateBackend(periodStart time.Time, flusher StatsFlusher, flushConfig *FlusherConfig, force bool) error {

	if !force {
		lastFlushedPeriod := s.getLastPeriodFlushed()
		if periodStart.Sub(lastFlushedPeriod) < defaultAggregationPeriod {
			s.c.Warningf("Refusing to update backend since it's too soon (last flush period %s, current period requested %s, aggregation period %s)", lastFlushedPeriod, periodStart, defaultAggregationPeriod)
			return ErrStatFlushTooSoon
		}
	}

	cfgMap, err := s.getActiveConfigs(periodStart, 0)
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
				var gm []float64
				if err := memcache.Gob.Unmarshal(item.Value, &gm); err != nil {
					s.c.Errorf("Bad data found in memcache: key %s, error: %s", k, err)
					continue
				}
				if len(gm) == 0 {
					panic("Something went terribly wrong; empty list cached!")
				}
				if cfgItem.Type == scTypeTiming {
					var median, sum, sumSquares float64
					// sort our list
					sort.Float64s(gm)
					count := len(gm)
					min := gm[0]
					max := gm[count-1]
					if count == 1 {
						median = gm[0]
					} else if count%2 == 0 {
						median = (gm[(count/2)-1] + gm[count/2]) / 2.0
					} else {
						median = gm[(count / 2)]
					}
					ninthdecileCount := int(math.Ceil(0.9 * float64(count)))
					ninthdecileValue := gm[ninthdecileCount-1]
					ninthdecileSum := 0.0
					for i, m := range gm {
						if i < ninthdecileCount {
							ninthdecileSum += m
						}
						sum += m
						sumSquares += math.Pow(m, 2.0)
					}
					datum = StatDataTiming{StatConfig: cfgItem, Count: count,
						Min: min, Max: max, Sum: sum, SumSquares: sumSquares,
						Median: median, NinthDecileCount: ninthdecileCount,
						NinthDecileSum: ninthdecileSum, NinthDecileValue: ninthdecileValue}
				} else {
					datum = StatDataGauge{StatConfig: cfgItem, Value: gm[0]}
				}
			case scTypeCounter:
				count, _ := strconv.ParseUint(string(item.Value), 10, 64)
				datum = StatDataCounter{StatConfig: cfgItem, Count: count}
			default:
				panic("If this happened, things are horribly wrong.")
			}
			data = append(data, datum)
		}

		if len(data) > 0 {
			// Now flush to the backend
			if err := flusher.Flush(data, flushConfig); err != nil {
				s.c.Errorf("Failed to flush to backend: %s", err)
				return err
			} else {
				s.updateLastPeriodFlushed(periodStart)
			}
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
		statConfigs[bucketKey] = sc
	}
	s.debugf("Found %d stat configs (cutoff time %s)", len(statConfigs), cutoffTime)
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

func (s StatImplementation) peekGauge(name, source string, at time.Time) ([]float64, error) {

	bucketKey, err := s.getBucketKey(scTypeGauge, name, source, time.Now())
	if err != nil {
		return nil, err
	}

	var gm []float64
	if _, err = memcache.Gob.Get(s.c, bucketKey, &gm); err != nil {
		return nil, err
	} else {
		return gm, nil
	}
}

func (s StatImplementation) peekTiming(name, source string, at time.Time) ([]float64, error) {

	bucketKey, err := s.getBucketKey(scTypeTiming, name, source, time.Now())
	if err != nil {
		return nil, err
	}

	var gm []float64
	if _, err = memcache.Gob.Get(s.c, bucketKey, &gm); err != nil {
		return nil, err
	} else {
		return gm, nil
	}
}

func (s StatImplementation) recordGaugeOrTiming(typ, name, source string, value, sampleRate float64) error {

	s.debugf("Recording %s/%s/%s: value=%f, samplerate=%f)", typ, name, source, value, sampleRate)

	if sampleRate < 1.0 && s.randGen.Float64() > sampleRate {
		s.debugf("Not recording value due to sampling rate")
		return ErrStatNotSampled // do nothing here, as we are sampling
	}

	now := time.Now()
	bucketKey, err := s.getBucketKey(typ, name, source, now)
	if err != nil {
		wrappedErr := NewErrStatDropped(typ, name, source, now, value, err)
		s.c.Warningf("%s (getting bucket key)", wrappedErr)
		return wrappedErr
	}

	var cached []float64
	var lastError error

	cachedItem, err := memcache.Gob.Get(s.c, bucketKey, &cached)
	if err == memcache.ErrCacheMiss {
		cached = make([]float64, 0)
		cachedItem = &memcache.Item{
			Key:        bucketKey,
			Expiration: time.Duration(2 * defaultAggregationPeriod),
		}
	} else if err != nil {
		wrappedErr := NewErrStatDropped(typ, name, source, now, value, err)
		s.c.Warningf("%s (getting value from memcache)", wrappedErr)
		return wrappedErr
	}

	switch typ {
	case scTypeTiming:
		cached = append(cached, value)
	case scTypeGauge:
		cached = []float64{value}
	}

	cachedItem.Object = &cached
	if err := memcache.Gob.Set(s.c, cachedItem); err != nil {
		wrappedErr := NewErrStatDropped(typ, name, source, now, value, lastError)
		s.c.Warningf("%s (failed to set value)", wrappedErr)
		return wrappedErr
	}
	return nil
}

func (s StatImplementation) getLastPeriodFlushed() time.Time {
	var lastPeriodFlushed time.Time
	if _, err := memcache.Gob.Get(s.c, "ss-lpf", &lastPeriodFlushed); err != nil {
		return time.Time{}
	}
	return lastPeriodFlushed
}

func (s StatImplementation) updateLastPeriodFlushed(lastPeriodFlushed time.Time) error {
	return memcache.Gob.Set(s.c, &memcache.Item{
		Key:    "ss-lpf",
		Object: &lastPeriodFlushed,
	})
}

func getStartOfFlushPeriod(at time.Time, offset int) time.Time {
	startOfPeriod := at.Truncate(defaultAggregationPeriod)
	if offset != 0 {
		startOfPeriod = startOfPeriod.Add(time.Duration(offset) * defaultAggregationPeriod)
	}
	return startOfPeriod
}

func (s StatImplementation) debugf(format string, args ...interface{}) {
	if s.debug {
		s.c.Debugf(format, args...)
	}
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
	Count            int
	Min              float64
	Max              float64
	Sum              float64
	SumSquares       float64
	Median           float64
	NinthDecileValue float64
	NinthDecileSum   float64
	NinthDecileCount int
}

func (dt StatDataTiming) String() string {
	return fmt.Sprintf("[Timing: name=%s, source=%s] Count: %d, Min: %f, Max: %f, Sum: %f, SumSquares: %f, Median: %f, 90th percentile (count: %d, value: %f, sum: %f):",
		dt.Name, dt.Source, dt.Count, dt.Min, dt.Max, dt.Sum, dt.SumSquares, dt.Median, dt.NinthDecileCount, dt.NinthDecileValue, dt.NinthDecileSum)
}

type StatDataGauge struct {
	StatConfig
	Value float64
}

func (dg StatDataGauge) String() string {
	return fmt.Sprintf("[Gauge: name=%s, source=%s] Value: %f",
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
