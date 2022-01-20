// Copyright 2021 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prometheus

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus/internal"
	dto "github.com/prometheus/client_model/go"
)

var _ TransactionalGatherer = &CachedTGatherer{}

// CachedTGatherer is a transactional gatherer that allows maintaining set of metrics which
// change less frequently than scrape time, yet label values and values change over time.
//
// If you happen to use NewDesc, NewConstMetric or MustNewConstMetric inside Collector.Collect routine, consider
// using CachedTGatherer instead.
//
// Use CachedTGatherer with classic Registry using NewMultiTRegistry and ToTransactionalGatherer helpers.
// TODO(bwplotka): Add non-session update API if useful for watcher-like mechanic.
type CachedTGatherer struct {
	metrics            map[uint64]*dto.Metric
	metricFamilyByName map[string]*dto.MetricFamily
	mMu                sync.RWMutex

	pendingSession bool
	psMu           sync.Mutex
}

func NewCachedTGatherer() *CachedTGatherer {
	return &CachedTGatherer{
		metrics:            make(map[uint64]*dto.Metric),
		metricFamilyByName: map[string]*dto.MetricFamily{},
	}
}

// Gather implements TransactionalGatherer interface.
func (c *CachedTGatherer) Gather() (_ []*dto.MetricFamily, done func(), err error) {
	c.mMu.RLock()
	// TODO(bwplotka): Consider caching slice and normalizing on write.
	return internal.NormalizeMetricFamilies(c.metricFamilyByName), c.mMu.RUnlock, nil
}

// NewSession allows to recreate state of all metrics in CachedTGatherer in
// one go and update cache in-place to save allocations.
// Only one session is allowed at the time.
//
// Session is not concurrency safe.
func (c *CachedTGatherer) NewSession() (*CollectSession, error) {
	c.psMu.Lock()
	if c.pendingSession {
		c.psMu.Unlock()
		return nil, errors.New("only one session allowed, one already pending")
	}
	c.pendingSession = true
	c.psMu.Unlock()

	return &CollectSession{
		c:              c,
		currentMetrics: make(map[uint64]*dto.Metric, len(c.metrics)),
		currentByName:  make(map[string]*dto.MetricFamily, len(c.metricFamilyByName)),
	}, nil
}

type CollectSession struct {
	closed bool

	c              *CachedTGatherer
	currentMetrics map[uint64]*dto.Metric
	currentByName  map[string]*dto.MetricFamily
}

// MustAddMetric is an AddMetric that panics on error.
func (s *CollectSession) MustAddMetric(fqName, help string, labelNames, labelValues []string, valueType ValueType, value float64, ts *time.Time) {
	if err := s.AddMetric(fqName, help, labelNames, labelValues, valueType, value, ts); err != nil {
		panic(err)
	}
}

// AddMetric adds metrics to current session. No changes will be updated in CachedTGatherer until Commit.
// TODO(bwplotka): Add validation.
func (s *CollectSession) AddMetric(fqName, help string, labelNames, labelValues []string, valueType ValueType, value float64, ts *time.Time) error {
	if s.closed {
		return errors.New("new metric: collect session is closed, but was attempted to be used")
	}

	// Label names can be unsorted, we will be sorting them later. The only implication is cachability if
	// consumer provide non-deterministic order of those.

	if len(labelNames) != len(labelValues) {
		return errors.New("new metric: label name has different len than values")
	}

	d, ok := s.currentByName[fqName]
	if !ok {
		d, ok = s.c.metricFamilyByName[fqName]
		if ok {
			d.Metric = d.Metric[:0]
		}
	}

	if !ok {
		// TODO(bwplotka): Validate?
		d = &dto.MetricFamily{}
		d.Name = proto.String(fqName)
		d.Type = valueType.ToDTO()
		d.Help = proto.String(help)
	} else {
		// TODO(bwplotka): Validate if same family?
		d.Type = valueType.ToDTO()
		d.Help = proto.String(help)
	}
	s.currentByName[fqName] = d

	h := xxhash.New()
	h.WriteString(fqName)
	h.Write(separatorByteSlice)

	for i := range labelNames {
		h.WriteString(labelNames[i])
		h.Write(separatorByteSlice)
		h.WriteString(labelValues[i])
		h.Write(separatorByteSlice)
	}
	hSum := h.Sum64()

	if _, ok := s.currentMetrics[hSum]; ok {
		return fmt.Errorf("found duplicate metric (same labels and values) to add %v", fqName)
	}
	m, ok := s.c.metrics[hSum]
	if !ok {
		m = &dto.Metric{
			Label: make([]*dto.LabelPair, 0, len(labelNames)),
		}
		for i := range labelNames {
			m.Label = append(m.Label, &dto.LabelPair{
				Name:  proto.String(labelNames[i]),
				Value: proto.String(labelValues[i]),
			})
		}
		sort.Sort(labelPairSorter(m.Label))
	}
	switch valueType {
	case CounterValue:
		v := m.Counter
		if v == nil {
			v = &dto.Counter{}
		}
		v.Value = proto.Float64(value)
		m.Counter = v
		m.Gauge = nil
		m.Untyped = nil
	case GaugeValue:
		v := m.Gauge
		if v == nil {
			v = &dto.Gauge{}
		}
		v.Value = proto.Float64(value)
		m.Counter = nil
		m.Gauge = v
		m.Untyped = nil
	case UntypedValue:
		v := m.Untyped
		if v == nil {
			v = &dto.Untyped{}
		}
		v.Value = proto.Float64(value)
		m.Counter = nil
		m.Gauge = nil
		m.Untyped = v
	default:
		return fmt.Errorf("unsupported value type %v", valueType)
	}

	m.TimestampMs = nil
	if ts != nil {
		m.TimestampMs = proto.Int64(ts.Unix()*1000 + int64(ts.Nanosecond()/1000000))
	}
	s.currentMetrics[hSum] = m

	// Will be sorted later anyway, skip for now.
	d.Metric = append(d.Metric, m)
	return nil
}

func (s *CollectSession) Commit() {
	s.c.mMu.Lock()
	// TODO(bwplotka): Sort metrics within family?
	s.c.metricFamilyByName = s.currentByName
	s.c.metrics = s.currentMetrics
	s.c.mMu.Unlock()

	s.c.psMu.Lock()
	s.closed = true
	s.c.pendingSession = false
	s.c.psMu.Unlock()
}

var _ TransactionalGatherer = &MultiTRegistry{}

// MultiTRegistry is a TransactionalGatherer that joins gathered metrics from multiple
// transactional gatherers.
//
// It is caller responsibility to ensure two registries have mutually exclusive metric families,
// no deduplication will happen.
type MultiTRegistry struct {
	tGatherers []TransactionalGatherer
}

// NewMultiTRegistry creates MultiTRegistry.
func NewMultiTRegistry(tGatherers ...TransactionalGatherer) *MultiTRegistry {
	return &MultiTRegistry{
		tGatherers: tGatherers,
	}
}

// Gather implements TransactionalGatherer interface.
func (r *MultiTRegistry) Gather() (mfs []*dto.MetricFamily, done func(), err error) {
	errs := MultiError{}

	dFns := make([]func(), 0, len(r.tGatherers))
	// TODO(bwplotka): Implement concurrency for those?
	for _, g := range r.tGatherers {
		// TODO(bwplotka): Check for duplicates?
		m, d, err := g.Gather()
		errs.Append(err)

		mfs = append(mfs, m...)
		dFns = append(dFns, d)
	}

	// TODO(bwplotka): Consider sort in place, given metric family in gather is sorted already.
	sort.Slice(mfs, func(i, j int) bool {
		return *mfs[i].Name < *mfs[j].Name
	})
	return mfs, func() {
		for _, d := range dFns {
			d()
		}
	}, errs.MaybeUnwrap()
}

// TransactionalGatherer represents transactional gatherer that can be triggered to notify gatherer that memory
// used by metric family is no longer used by a caller. This allows implementations with cache.
type TransactionalGatherer interface {
	// Gather returns metrics in a lexicographically sorted slice
	// of uniquely named MetricFamily protobufs. Gather ensures that the
	// returned slice is valid and self-consistent so that it can be used
	// for valid exposition. As an exception to the strict consistency
	// requirements described for metric.Desc, Gather will tolerate
	// different sets of label names for metrics of the same metric family.
	//
	// Even if an error occurs, Gather attempts to gather as many metrics as
	// possible. Hence, if a non-nil error is returned, the returned
	// MetricFamily slice could be nil (in case of a fatal error that
	// prevented any meaningful metric collection) or contain a number of
	// MetricFamily protobufs, some of which might be incomplete, and some
	// might be missing altogether. The returned error (which might be a
	// MultiError) explains the details. Note that this is mostly useful for
	// debugging purposes. If the gathered protobufs are to be used for
	// exposition in actual monitoring, it is almost always better to not
	// expose an incomplete result and instead disregard the returned
	// MetricFamily protobufs in case the returned error is non-nil.
	//
	// Important: done is expected to be triggered (even if the error occurs!)
	// once caller does not need returned slice of dto.MetricFamily.
	Gather() (_ []*dto.MetricFamily, done func(), err error)
}

// ToTransactionalGatherer transforms Gatherer to transactional one with noop as done function.
func ToTransactionalGatherer(g Gatherer) TransactionalGatherer {
	return &noTransactionGatherer{g: g}
}

type noTransactionGatherer struct {
	g Gatherer
}

// Gather implements TransactionalGatherer interface.
func (g *noTransactionGatherer) Gather() (_ []*dto.MetricFamily, done func(), err error) {
	mfs, err := g.g.Gather()
	return mfs, func() {}, err
}
