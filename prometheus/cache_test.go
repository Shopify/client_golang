// Copyright 2022 The Prometheus Authors
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
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
)

func TestCachedTGatherer(t *testing.T) {
	c := NewCachedTGatherer()
	mfs, done, err := c.Gather()
	if err != nil {
		t.Error("gather failed:", err)
	}
	done()
	if got := mfsToString(mfs); got != "" {
		t.Error("unexpected metric family", got)
	}

	s, err := c.NewSession()
	if err != nil {
		t.Error("session failed:", err)
	}

	_, err = c.NewSession()
	if err == nil {
		t.Error("second session expected to fail, got nil")
	}

	if err := s.AddMetric("a", "help a", []string{"b", "c"}, []string{"valb", "valc"}, GaugeValue, 1, nil); err != nil {
		t.Error("add metric:", err)
	}

	// Without commit we should see still empty list.
	mfs, done, err = c.Gather()
	if err != nil {
		t.Error("gather failed:", err)
	}
	done()
	if got := mfsToString(mfs); got != "" {
		t.Error("unexpected metric family", got)
	}

	s.Commit()
	mfs, done, err = c.Gather()
	if err != nil {
		t.Error("gather failed:", err)
	}
	done()
	if got := mfsToString(mfs); got != "name:\"a\" help:\"help a\" type:GAUGE metric:<label:<name:\"b\" value:\"valb\" > label:<name:\"c\" value:\"valc\" > gauge:<value:1 > > " {
		t.Error("unexpected metric family, got", got)
	}
}

func mfsToString(mfs []*dto.MetricFamily) string {
	ret := make([]string, 0, len(mfs))
	for _, m := range mfs {
		ret = append(ret, m.String())
	}
	return strings.Join(ret, ",")
}

type tGatherer struct {
	done bool
	err  error
}

func (g *tGatherer) Gather() (_ []*dto.MetricFamily, done func(), err error) {
	name := "g1"
	val := 1.0
	return []*dto.MetricFamily{
		{Name: &name, Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: &val}}}},
	}, func() { g.done = true }, g.err
}

func TestNewMultiTRegistry(t *testing.T) {
	treg := &tGatherer{}

	t.Run("one registry", func(t *testing.T) {
		m := NewMultiTRegistry(treg)
		ret, done, err := m.Gather()
		if err != nil {
			t.Error("gather failed:", err)
		}
		done()
		if len(ret) != 1 {
			t.Error("unexpected number of metric families, expected 1, got", ret)
		}
		if !treg.done {
			t.Error("inner transactional registry not marked as done")
		}
	})

	reg := NewRegistry()
	if err := reg.Register(NewCounter(CounterOpts{Name: "c1", Help: "help c1"})); err != nil {
		t.Error("registration failed:", err)
	}

	// Note on purpose two registries will have exactly same metric family name (but with different string).
	// This behaviour is undefined at the moment.
	if err := reg.Register(NewGauge(GaugeOpts{Name: "g1", Help: "help g1"})); err != nil {
		t.Error("registration failed:", err)
	}
	treg.done = false

	t.Run("two registries", func(t *testing.T) {
		m := NewMultiTRegistry(ToTransactionalGatherer(reg), treg)
		ret, done, err := m.Gather()
		if err != nil {
			t.Error("gather failed:", err)
		}
		done()
		if len(ret) != 3 {
			t.Error("unexpected number of metric families, expected 3, got", ret)
		}
		if !treg.done {
			t.Error("inner transactional registry not marked as done")
		}
	})

	treg.done = false
	// Inject error.
	treg.err = errors.New("test err")

	t.Run("two registries, one with error", func(t *testing.T) {
		m := NewMultiTRegistry(ToTransactionalGatherer(reg), treg)
		ret, done, err := m.Gather()
		if err != treg.err {
			t.Error("unexpected error:", err)
		}
		done()
		if len(ret) != 3 {
			t.Error("unexpected number of metric families, expected 3, got", ret)
		}
		// Still on error, we expect done to be triggered.
		if !treg.done {
			t.Error("inner transactional registry not marked as done")
		}
	})
}
