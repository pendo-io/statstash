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
	"fmt"
	"strconv"
	"time"
)

const (
	dsKindStatConfig         = "StatConfig"
	scTypeGauge              = "gauge"
	scTypeCounter            = "counter"
	defaultAggregationPeriod = time.Duration(5 * time.Minute)
)

type gaugeMetrics []gaugeMetric

type gaugeMetric struct {
	Timestamp int64       `json:"ts"`
	Value     interface{} `json:"v"`
}

type StatConfig struct {
	Name     string    `datastore:",noindex" json:"name"`
	Source   string    `datastore:",noindex" json:"source"`
	Type     string    `datastore:",noindex" json:"type"`
	Period   int64     `datastore:",noindex" json:"period"`
	LastRead time.Time `json:"lastread"`
}

func (sc StatConfig) String() string {
	return fmt.Sprintf("[StatConfig] name=%s, source=%s, type=%s, period=%s, lastread=%s",
		sc.Name, sc.Source, sc.Type, sc.Period, sc.LastRead)
}

func (sc StatConfig) BucketKey(t time.Time) string {
	return fmt.Sprintf("statstash-metric:%s-%s-%s",
		sc.Name, sc.Source, t.Truncate(time.Duration(sc.Period)).Unix())
}

// StatInterface defines the interface for the application to
type StatInterface interface {
	IncrementCounter(name, source string) error
	IncrementCounterBy(name, source string, delta int64) error
	RecordGauge(name, source string, value interface{}) error
}

type StatInterfaceImplementation struct {
	c appengine.Context
}

func (s StatInterfaceImplementation) IncrementCounter(name, source string) error {
	return s.IncrementCounterBy(name, source, 1)
}

func (s StatInterfaceImplementation) IncrementCounterBy(name, source string, delta int64) error {
	statConfig, err := s.getStatConfig(name, source, scTypeCounter)
	if err != nil {
		return err
	}

	now := time.Now()
	bucketKey := statConfig.BucketKey(now)

	if _, err = memcache.Increment(s.c, bucketKey, delta, 0); err != nil {
		s.c.Warningf("Failed to increment %s delta %d", bucketKey, delta)
	}

	return err
}

func (s StatInterfaceImplementation) peekCounter(name, source string, at time.Time) (uint64, error) {
	statConfig, err := s.getStatConfig(name, source, scTypeCounter)
	if err != nil {
		return uint64(0), err
	}

	bucketKey := statConfig.BucketKey(at)

	if item, err := memcache.Get(s.c, bucketKey); err == nil {
		return strconv.ParseUint(string(item.Value), 10, 64)
	} else {
		return uint64(0), err
	}
}

func (s StatInterfaceImplementation) peekGauge(name, source string, at time.Time) (gaugeMetrics, error) {
	statConfig, err := s.getStatConfig(name, source, scTypeCounter)
	if err != nil {
		return nil, err
	}

	bucketKey := statConfig.BucketKey(at)
	var gm gaugeMetrics
	if _, err = memcache.JSON.Get(s.c, bucketKey, &gm); err != nil {
		return nil, err
	} else {
		return gm, nil
	}
}

func (s StatInterfaceImplementation) RecordGauge(name, source string, value interface{}) error {
	statConfig, err := s.getStatConfig(name, source, scTypeGauge)
	if err != nil {
		return err
	}

	now := time.Now()
	bucketKey := statConfig.BucketKey(now)

	var cachedMetrics gaugeMetrics

	for tries := 0; tries < 5; tries++ {
		cachedItem, err := memcache.JSON.Get(s.c, bucketKey, &cachedMetrics)
		if err == memcache.ErrCacheMiss {
			cachedMetrics = make(gaugeMetrics, 0)
		} else if err != nil {
			// give up fast because something is wrong with memcache, probably
			return err
		}

		cachedMetrics = append(cachedMetrics, gaugeMetric{Timestamp: now.Unix(), Value: value})

		if cachedItem != nil {
			cachedItem.Object = &cachedMetrics
			if err := memcache.JSON.CompareAndSwap(s.c, cachedItem); err == nil {
				return nil
			} else if err == memcache.ErrCASConflict {
				time.Sleep(time.Microsecond * 50)
				continue // do it again from the top
			} else if err == memcache.ErrNotStored {
				if err := memcache.JSON.Add(s.c, cachedItem); err == memcache.ErrNotStored {
					return nil
				}
			} else {
				return err // something went horribly wrong, bail
			}
		}

		// If we made it this far, we'll need to just make a new item and store it
		newItem := &memcache.Item{
			Key:        bucketKey,
			Object:     &cachedMetrics,
			Expiration: time.Duration(2 * defaultAggregationPeriod),
		}
		if err := memcache.JSON.Add(s.c, newItem); err == nil {
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

func (s StatInterfaceImplementation) getStatConfigKeyName(name, source string) string {
	if source != "" {
		return fmt.Sprintf("%s-%s", name, source)
	}
	return name
}

func (s StatInterfaceImplementation) getStatConfigMemcacheKey(name, source string) string {
	return fmt.Sprintf("statstash-conf:%s", s.getStatConfigKeyName(name, source))
}

func (s StatInterfaceImplementation) getStatConfigDatastoreKey(name, source string) *datastore.Key {
	return datastore.NewKey(s.c, dsKindStatConfig, s.getStatConfigKeyName(name, source), 0, nil)
}

func (s StatInterfaceImplementation) getStatConfig(name, source, typ string) (StatConfig, error) {

	var sc StatConfig

	// First, query memcache
	if _, err := memcache.JSON.Get(s.c, s.getStatConfigMemcacheKey(name, source), &sc); err == nil {
		return sc, nil
	}

	k := s.getStatConfigDatastoreKey(name, source)
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
		sc.Period = int64(defaultAggregationPeriod)
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
		memcache.JSON.Add(s.c, &memcache.Item{
			Key:        s.getStatConfigMemcacheKey(name, source),
			Object:     &sc,
			Expiration: time.Duration(24 * time.Hour),
		})
	}

	return sc, nil

}

// StatsFlusher is an interface used to flush stats to various locations
type StatsFlusher interface {
	Flush() error
}

// LogOnlyStatsFlusher is used to "flush" stats for testing and development.
// Stats that are flushed are logged only.
type LogOnlyStatsFlusher struct {
	c appengine.Context
}

// LibratoStatsFlusher is used to flush stats to the Librato metrics service.
type LibratoStatsFlusher struct {
	c appengine.Context
}
