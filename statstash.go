// Copyright 2014-2015 pendo.io
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
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"time"

	"github.com/pendo-io/appwrap"
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

func NewStatInterface(log appwrap.Logging, ds appwrap.Datastore, cache appwrap.Memcache, debug bool) StatInterface {
	return StatImplementation{
		log:     log,
		ds:      ds,
		cache:   cache,
		randGen: rand.New(rand.NewSource(time.Now().UnixNano())),
		debug:   debug,
	}
}

type StatImplementation struct {
	log     appwrap.Logging
	ds      appwrap.Datastore
	cache   appwrap.Memcache
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
	s.log.Debugf("record bucketKey: %s", bucketKey)

	if _, err = s.cache.Increment(bucketKey, delta, 0); err != nil {
		s.log.Warningf("Failed to increment %s delta %d", bucketKey, delta)
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
			s.log.Warningf("Refusing to update backend since it's too soon (last flush period %s, current period requested %s, aggregation period %s)", lastFlushedPeriod, periodStart, defaultAggregationPeriod)
			return ErrStatFlushTooSoon
		}
	}

	cfgMap, err := s.getActiveConfigs(periodStart, 0)
	if err != nil {
		s.log.Errorf("Failed to get active buckets when updating backend: %s", err)
		return err
	}

	if len(cfgMap) == 0 {
		return nil // nothing to do
	}

	bucketKeys := make([]string, 0, len(cfgMap))
	for k := range cfgMap {
		bucketKeys = append(bucketKeys, k)
	}

	if itemMap, err := s.cache.GetMulti(bucketKeys); err != nil {
		s.log.Errorf("Failed to fetch items from memcache when updating backend: %s", err)
	} else {
		// Get our data from memcache in one go
		data := make([]interface{}, 0, len(itemMap))
		for k, item := range itemMap {
			var datum interface{}
			cfgItem := cfgMap[k]
			switch cfgItem.Type {
			case scTypeTiming, scTypeGauge:
				var gm []float64
				if err := s.gobUnmarshal(item.Value, &gm); err != nil {
					s.log.Errorf("Bad data found in memcache: key %s, error: %s", k, err)
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
				s.log.Errorf("Failed to flush to backend: %s", err)
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
	dsKeys := make([]*appwrap.DatastoreKey, 0, len(sc))
	memcacheKeys := make([]string, 0, len(sc))
	for _, cfg := range sc {
		dsKeys = append(dsKeys, s.getStatConfigDatastoreKey(cfg.Type, cfg.Name, cfg.Source))
		memcacheKeys = append(memcacheKeys, cfg.BucketKey(now, 0))
		memcacheKeys = append(memcacheKeys, cfg.BucketKey(now, -1))

	}

	if err := s.ds.DeleteMulti(dsKeys); err != nil {
		s.log.Errorf("Stats: purge datastore failed: %s", err)
		return err
	}

	s.cache.DeleteMulti(memcacheKeys)
	return nil
}

func (s StatImplementation) getAllConfigs() ([]StatConfig, error) {
	q := s.ds.NewQuery(dsKindStatConfig)
	var cfgs []StatConfig
	_, err := q.GetAll(&cfgs)
	return cfgs, err
}

func (s StatImplementation) getActiveConfigs(at time.Time, offset int) (map[string]StatConfig, error) {

	statConfigs := make(map[string]StatConfig)

	var finalError error
	cutoffTime := at.Add(time.Duration(time.Hour * 24 * -2))

	q := s.ds.NewQuery(dsKindStatConfig).Filter("LastRead >", cutoffTime)
	iter := q.Run()
	for {
		var sc StatConfig
		_, err := iter.Next(&sc)
		if err == appwrap.DatastoreDone {
			break // end of iteration
		} else if err != nil {
			s.log.Warningf("Failed iterating stat config items to get active buckets: %s", err)
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

func (s StatImplementation) getStatConfigDatastoreKey(typ, name, source string) *appwrap.DatastoreKey {
	return s.ds.NewKey(dsKindStatConfig, s.getStatConfigKeyName(typ, name, source), 0, nil)
}

func (s StatImplementation) getStatConfig(typ, name, source string) (StatConfig, error) {

	var sc StatConfig

	// First, query memcache
	if item, err := s.cache.Get(s.getStatConfigMemcacheKey(typ, name, source)); err == nil {
		if err := s.gobUnmarshal(item.Value, &sc); err != nil {
			return StatConfig{}, err
		} else {
			return sc, nil
		}
	}

	k := s.getStatConfigDatastoreKey(typ, name, source)
	now := time.Now()
	cache := true

	// Now query datastore
	if err := s.ds.Get(k, &sc); err != nil && err != appwrap.ErrNoSuchEntity {
		return StatConfig{}, err
	} else if err == appwrap.ErrNoSuchEntity {
		sc.Name = name
		sc.Source = source
		sc.Type = typ
	}

	sc.LastRead = now

	// Store item in datastore if it needed the update
	if _, err := s.ds.Put(k, &sc); err != nil {
		s.log.Warningf("Failed to update StatConfig %s: %s", sc, err)
		cache = false
	}

	// Only attempt adding if the update was needed and succeeded
	if cache {
		if b, err := s.gobMarshal(&sc); err != nil {
			s.log.Warningf("Failed to encode stat config item into memcache: %s", err)
			return StatConfig{}, nil
		} else {
			s.cache.Add(&appwrap.CacheItem{
				Key:        s.getStatConfigMemcacheKey(typ, name, source),
				Value:      b,
				Expiration: time.Duration(24 * time.Hour),
			})
		}
	}

	return sc, nil

}

func (s StatImplementation) peekCounter(name, source string, at time.Time) (uint64, error) {

	bucketKey, err := s.getBucketKey(scTypeCounter, name, source, time.Now())
	if err != nil {
		return uint64(0), err
	}

	s.log.Debugf("peek bucketKey: %s", bucketKey)

	if item, err := s.cache.Get(bucketKey); err == nil {
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
	if item, err := s.cache.Get(bucketKey); err != nil {
		return nil, err
	} else {
		if s.gobUnmarshal(item.Value, &gm); err != nil {
			s.log.Errorf("Error decoding gauge values: %s", err)
			return nil, err
		}
		return gm, nil
	}
}

func (s StatImplementation) peekTiming(name, source string, at time.Time) ([]float64, error) {

	bucketKey, err := s.getBucketKey(scTypeTiming, name, source, time.Now())
	if err != nil {
		return nil, err
	}

	var gm []float64
	if item, err := s.cache.Get(bucketKey); err != nil {
		return nil, err
	} else {
		if s.gobUnmarshal(item.Value, &gm); err != nil {
			s.log.Errorf("Error decoding timing values: %s", err)
			return nil, err
		}
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
		s.log.Warningf("%s (getting bucket key)", wrappedErr)
		return wrappedErr
	}

	s.log.Debugf("record bucketKey: %s", bucketKey)

	var cached []float64

	cachedItem, err := s.cache.Get(bucketKey)
	if err == appwrap.ErrCacheMiss {
		cached = make([]float64, 0)
		cachedItem = &appwrap.CacheItem{
			Key:        bucketKey,
			Expiration: time.Duration(2 * defaultAggregationPeriod),
		}
	} else if err != nil {
		wrappedErr := NewErrStatDropped(typ, name, source, now, value, err)
		s.log.Warningf("%s (getting value from memcache)", wrappedErr)
		return wrappedErr
	} else {
		if s.gobUnmarshal(cachedItem.Value, &cached); err != nil {
			wrappedErr := NewErrStatDropped(typ, name, source, now, value, err)
			s.log.Warningf("%s (decoding value from memcache)", wrappedErr)
			return wrappedErr
		}
	}

	switch typ {
	case scTypeTiming:
		cached = append(cached, value)
	case scTypeGauge:
		cached = []float64{value}
	}

	if b, err := s.gobMarshal(&cached); err != nil {
		wrappedErr := NewErrStatDropped(typ, name, source, now, value, err)
		s.log.Warningf("%s (failed to encode new value)", wrappedErr)
		return wrappedErr
	} else {
		cachedItem.Value = b
		if err := s.cache.Set(cachedItem); err != nil {
			wrappedErr := NewErrStatDropped(typ, name, source, now, value, err)
			s.log.Warningf("%s (failed to set value)", wrappedErr)
			return wrappedErr
		}
	}
	return nil
}

func (s StatImplementation) getLastPeriodFlushed() time.Time {
	var lastPeriodFlushed time.Time
	if item, err := s.cache.Get("ss-lpf"); err != nil {
		return time.Time{}
	} else {
		if err := s.gobUnmarshal(item.Value, &lastPeriodFlushed); err != nil {
			s.log.Errorf("Failed to get last period flushed: %s", err)
			return time.Time{}
		}
	}
	s.log.Debugf("lastPeriodFlushed %s", lastPeriodFlushed)
	return lastPeriodFlushed
}

func (s StatImplementation) updateLastPeriodFlushed(lastPeriodFlushed time.Time) error {
	if b, err := s.gobMarshal(&lastPeriodFlushed); err != nil {
		s.log.Errorf("Failed to set last period flushed: %s", err)
		return err
	} else {
		s.log.Debugf("FOOOO")
		return s.cache.Set(&appwrap.CacheItem{
			Key:   "ss-lpf",
			Value: b,
		})
	}
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
		s.log.Debugf(format, args...)
	}
}

func (s StatImplementation) gobMarshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s StatImplementation) gobUnmarshal(data []byte, v interface{}) error {
	return gob.NewDecoder(bytes.NewBuffer(data)).Decode(v)
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
	log appwrap.Logging
}

func NewLogOnlyStatsFlusher(log appwrap.Logging) StatsFlusher {
	return LogOnlyStatsFlusher{log}
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
		f.log.Infof("%s", datum)
	}
	return nil
}
