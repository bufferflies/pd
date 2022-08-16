// Copyright 2019 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package operator

import (
	"fmt"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/core/storelimit"
)

// OpInfluence records the influence of the cluster.
type OpInfluence struct {
	StoresInfluence map[uint64]*StoreInfluence
}

func (m OpInfluence) String() string {
	str := ""
	for id, influence := range m.StoresInfluence {
		s := fmt.Sprintf("[store-id:%d,step-cost-add-peer:%d, remove-peer:%d,recv-snap:%d,send-snap:%d]",
			id,
			influence.GetStepCost(storelimit.AddPeer),
			influence.GetStepCost(storelimit.RemovePeer),
			influence.GetSnapCost(storelimit.RecvSnapShot),
			influence.GetSnapCost(storelimit.SendSnapShot),
		)
		str += s
	}
	return str
}

// NewOpInfluence is the constructor of the OpInfluence.
func NewOpInfluence() *OpInfluence {
	return &OpInfluence{StoresInfluence: make(map[uint64]*StoreInfluence)}
}

// Add adds another influence.
func (m *OpInfluence) Add(other *OpInfluence) {
	for id, v := range other.StoresInfluence {
		m.GetStoreInfluence(id).Add(v)
	}
}

// Sub subs another influence.
func (m *OpInfluence) Sub(other *OpInfluence) {
	for id, v := range other.StoresInfluence {
		m.GetStoreInfluence(id).Sub(v)
	}
}

// GetStoreInfluence get storeInfluence of specific store.
func (m OpInfluence) GetStoreInfluence(id uint64) *StoreInfluence {
	storeInfluence, ok := m.StoresInfluence[id]
	if !ok {
		storeInfluence = &StoreInfluence{}
		m.StoresInfluence[id] = storeInfluence
	}
	return storeInfluence
}

// StoreInfluence records influences that pending operators will make.
type StoreInfluence struct {
	RegionSize  int64
	RegionCount int64
	LeaderSize  int64
	LeaderCount int64
	StepCost    map[storelimit.Type]int64
	SnapCost    map[storelimit.SnapType]int64
}

func (s *StoreInfluence) Add(other *StoreInfluence) {
	s.RegionCount += other.RegionCount
	s.RegionSize += other.RegionSize
	s.LeaderSize += other.LeaderSize
	s.LeaderCount += other.LeaderCount
	for _, v := range storelimit.TypeNameValue {
		s.addStepCost(v, other.GetStepCost(v))
	}
	for _, v := range storelimit.SnapTypeNameValue {
		s.AddSnapCost(v, other.GetSnapCost(v))
	}
}

func (s *StoreInfluence) Sub(other *StoreInfluence) {
	s.RegionCount -= other.RegionCount
	s.RegionSize -= other.RegionSize
	s.LeaderSize -= other.LeaderSize
	s.LeaderCount -= other.LeaderCount
	for _, v := range storelimit.TypeNameValue {
		s.addStepCost(v, -other.GetStepCost(v))
	}
	for _, v := range storelimit.SnapTypeNameValue {
		s.AddSnapCost(v, -other.GetSnapCost(v))
	}
}

// ResourceProperty returns delta size of leader/region by influence.
func (s StoreInfluence) ResourceProperty(kind core.ScheduleKind) int64 {
	switch kind.Resource {
	case core.LeaderKind:
		switch kind.Policy {
		case core.ByCount:
			return s.LeaderCount
		case core.BySize:
			return s.LeaderSize
		default:
			return 0
		}
	case core.RegionKind:
		return s.RegionSize
	default:
		return 0
	}
}

// GetSnapCost returns the given snapshot size.
func (s StoreInfluence) GetSnapCost(snapType storelimit.SnapType) int64 {
	if s.SnapCost == nil {
		return 0
	}
	return s.SnapCost[snapType]
}

// GetStepCost returns the specific type step cost
func (s StoreInfluence) GetStepCost(limitType storelimit.Type) int64 {
	if s.StepCost == nil {
		return 0
	}
	return s.StepCost[limitType]
}

// AddSnapCost adds the step cost of specific type store limit according to region size.
func (s *StoreInfluence) AddSnapCost(limitType storelimit.SnapType, cost int64) {
	if s.SnapCost == nil {
		s.SnapCost = make(map[storelimit.SnapType]int64)
	}
	s.SnapCost[limitType] += cost
}

func (s *StoreInfluence) addStepCost(limitType storelimit.Type, cost int64) {
	if s.StepCost == nil {
		s.StepCost = make(map[storelimit.Type]int64)
	}
	s.StepCost[limitType] += cost
}

// AdjustStepCost adjusts the step cost of specific type store limit according to region size
func (s *StoreInfluence) AdjustStepCost(limitType storelimit.Type, regionSize int64) {
	if regionSize > storelimit.SmallRegionThreshold {
		s.addStepCost(limitType, storelimit.RegionInfluence[limitType])
	} else if regionSize > core.EmptyRegionApproximateSize {
		s.addStepCost(limitType, storelimit.SmallRegionInfluence[limitType])
	}
}
