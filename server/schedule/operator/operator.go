// Copyright 2016 TiKV Project Authors.
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
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/server/core"
)

const (
	// OperatorExpireTime is the duration that when an operator is not started
	// after it, the operator will be considered expired.
	OperatorExpireTime = 3 * time.Second
	// FastOperatorWaitTime is the duration that when an operator that is not marked
	// `OpRegion` runs longer than it, the operator will be considered timeout.
	FastOperatorWaitTime = 10 * time.Second
	// SlowOperatorWaitTime is the duration that when an operator marked `OpRegion`
	// runs longer than it, the operator will be considered timeout.
	SlowOperatorWaitTime = 10 * time.Minute
)

// Operator contains execution steps generated by scheduler.
type Operator struct {
	desc             string
	brief            string
	regionID         uint64
	regionEpoch      *metapb.RegionEpoch
	kind             OpKind
	steps            []OpStep
	stepsTime        []int64 // step finish time
	currentStep      int32
	status           OpStatusTracker
	level            core.PriorityLevel
	Counters         []prometheus.Counter
	FinishedCounters []prometheus.Counter
	AdditionalInfos  map[string]string
}

// NewOperator creates a new operator.
func NewOperator(desc, brief string, regionID uint64, regionEpoch *metapb.RegionEpoch, kind OpKind, steps ...OpStep) *Operator {
	level := core.NormalPriority
	if kind&OpAdmin != 0 {
		level = core.HighPriority
	}
	return &Operator{
		desc:            desc,
		brief:           brief,
		regionID:        regionID,
		regionEpoch:     regionEpoch,
		kind:            kind,
		steps:           steps,
		stepsTime:       make([]int64, len(steps)),
		status:          NewOpStatusTracker(),
		level:           level,
		AdditionalInfos: make(map[string]string),
	}
}

func (o *Operator) String() string {
	stepStrs := make([]string, len(o.steps))
	for i := range o.steps {
		stepStrs[i] = o.steps[i].String()
	}
	s := fmt.Sprintf("%s {%s} (kind:%s, region:%v(%v,%v), createAt:%s, startAt:%s, currentStep:%v，steps:[%s])",
		o.desc, o.brief, o.kind, o.regionID, o.regionEpoch.GetVersion(), o.regionEpoch.GetConfVer(), o.GetCreateTime(),
		o.GetStartTime(), atomic.LoadInt32(&o.currentStep), strings.Join(stepStrs, ", "))
	if o.CheckSuccess() {
		s += " finished"
	}
	if o.CheckTimeout() {
		s += " timeout"
	}
	return s
}

// MarshalJSON serializes custom types to JSON.
func (o *Operator) MarshalJSON() ([]byte, error) {
	return []byte(`"` + o.String() + `"`), nil
}

// Desc returns the operator's short description.
func (o *Operator) Desc() string {
	return o.desc
}

// SetDesc sets the description for the operator.
func (o *Operator) SetDesc(desc string) {
	o.desc = desc
}

// AttachKind attaches an operator kind for the operator.
func (o *Operator) AttachKind(kind OpKind) {
	o.kind |= kind
}

// RegionID returns the region that operator is targeted.
func (o *Operator) RegionID() uint64 {
	return o.regionID
}

// RegionEpoch returns the region's epoch that is attached to the operator.
func (o *Operator) RegionEpoch() *metapb.RegionEpoch {
	return o.regionEpoch
}

// Kind returns operator's kind.
func (o *Operator) Kind() OpKind {
	return o.kind
}

// SchedulerKind return the highest OpKind even if the operator has many OpKind
// fix #3778
func (o *Operator) SchedulerKind() OpKind {
	// LowBit ref: https://en.wikipedia.org/wiki/Find_first_set
	// 6(110) ==> 2(10)
	// 5(101) ==> 1(01)
	// 4(100) ==> 4(100)
	return o.kind & (-o.kind)
}

// Status returns operator status.
func (o *Operator) Status() OpStatus {
	return o.status.Status()
}

// CheckAndGetStatus returns operator status after `CheckExpired` and `CheckTimeout`.
func (o *Operator) CheckAndGetStatus() OpStatus {
	switch {
	case o.CheckExpired():
		return EXPIRED
	case o.CheckTimeout():
		return TIMEOUT
	default:
		return o.Status()
	}
}

// GetReachTimeOf returns the time when operator reaches the given status.
func (o *Operator) GetReachTimeOf(st OpStatus) time.Time {
	return o.status.ReachTimeOf(st)
}

// GetCreateTime gets the create time of operator.
func (o *Operator) GetCreateTime() time.Time {
	return o.status.ReachTimeOf(CREATED)
}

// ElapsedTime returns duration since it was created.
func (o *Operator) ElapsedTime() time.Duration {
	return time.Since(o.GetCreateTime())
}

// Start sets the operator to STARTED status, returns whether succeeded.
func (o *Operator) Start() bool {
	return o.status.To(STARTED)
}

// HasStarted returns whether operator has started.
func (o *Operator) HasStarted() bool {
	return !o.GetStartTime().IsZero()
}

// GetStartTime gets the start time of operator.
func (o *Operator) GetStartTime() time.Time {
	return o.status.ReachTimeOf(STARTED)
}

// RunningTime returns duration since it started.
func (o *Operator) RunningTime() time.Duration {
	if o.HasStarted() {
		return time.Since(o.GetStartTime())
	}
	return 0
}

// IsEnd checks if the operator is at and end status.
func (o *Operator) IsEnd() bool {
	return o.status.IsEnd()
}

// CheckSuccess checks if all steps are finished, and update the status.
func (o *Operator) CheckSuccess() bool {
	if atomic.LoadInt32(&o.currentStep) >= int32(len(o.steps)) {
		return o.status.To(SUCCESS) || o.Status() == SUCCESS
	}
	return false
}

// Cancel marks the operator canceled.
func (o *Operator) Cancel() bool {
	return o.status.To(CANCELED)
}

// Replace marks the operator replaced.
func (o *Operator) Replace() bool {
	return o.status.To(REPLACED)
}

// CheckExpired checks if the operator is expired, and update the status.
func (o *Operator) CheckExpired() bool {
	return o.status.CheckExpired(OperatorExpireTime)
}

// CheckTimeout checks if the operator is timeout, and update the status.
func (o *Operator) CheckTimeout() bool {
	if o.CheckSuccess() {
		return false
	}
	currentStep := int(atomic.LoadInt32(&o.currentStep))
	var startTime time.Time
	if currentStep == 0 {
		startTime = o.GetStartTime()
	} else {
		startTime = time.Unix(0, atomic.LoadInt64(&(o.stepsTime[currentStep-1])))
	}
	step := o.steps[currentStep]
	return o.status.CheckStepTimeout(startTime, step)
}

// Len returns the operator's steps count.
func (o *Operator) Len() int {
	return len(o.steps)
}

// Step returns the i-th step.
func (o *Operator) Step(i int) OpStep {
	if i >= 0 && i < len(o.steps) {
		return o.steps[i]
	}
	return nil
}

// Check checks if current step is finished, returns next step to take action.
// If operator is at an end status, check returns nil.
// It's safe to be called by multiple goroutine concurrently.
func (o *Operator) Check(region *core.RegionInfo) OpStep {
	if o.IsEnd() {
		return nil
	}
	// CheckTimeout will call CheckSuccess first
	defer func() { _ = o.CheckTimeout() }()
	for step := atomic.LoadInt32(&o.currentStep); int(step) < len(o.steps); step++ {
		if o.steps[int(step)].IsFinish(region) {
			if atomic.CompareAndSwapInt64(&(o.stepsTime[step]), 0, time.Now().UnixNano()) {
				var startTime time.Time
				if step == 0 {
					startTime = o.GetStartTime()
				} else {
					startTime = time.Unix(0, atomic.LoadInt64(&(o.stepsTime[step-1])))
				}
				operatorStepDuration.WithLabelValues(reflect.TypeOf(o.steps[int(step)]).Name()).
					Observe(time.Unix(0, o.stepsTime[step]).Sub(startTime).Seconds())
			}
			atomic.StoreInt32(&o.currentStep, step+1)
		} else {
			return o.steps[int(step)]
		}
	}
	return nil
}

// ConfVerChanged returns the number of confver has consumed by steps
func (o *Operator) ConfVerChanged(region *core.RegionInfo) (total uint64) {
	current := atomic.LoadInt32(&o.currentStep)
	if current == int32(len(o.steps)) {
		current--
	}
	// including current step, it may has taken effects in this heartbeat
	for _, step := range o.steps[0 : current+1] {
		total += step.ConfVerChanged(region)
	}
	return total
}

// SetPriorityLevel sets the priority level for operator.
func (o *Operator) SetPriorityLevel(level core.PriorityLevel) {
	o.level = level
}

// GetPriorityLevel gets the priority level.
func (o *Operator) GetPriorityLevel() core.PriorityLevel {
	return o.level
}

// UnfinishedInfluence calculates the store difference which unfinished operator steps make.
func (o *Operator) UnfinishedInfluence(opInfluence OpInfluence, region *core.RegionInfo) {
	for step := atomic.LoadInt32(&o.currentStep); int(step) < len(o.steps); step++ {
		if !o.steps[int(step)].IsFinish(region) {
			o.steps[int(step)].Influence(opInfluence, region)
		}
	}
}

// TotalInfluence calculates the store difference which whole operator steps make.
func (o *Operator) TotalInfluence(opInfluence OpInfluence, region *core.RegionInfo) {
	for step := 0; step < len(o.steps); step++ {
		o.steps[step].Influence(opInfluence, region)
	}
}

// OpHistory is used to log and visualize completed operators.
type OpHistory struct {
	FinishTime time.Time
	From, To   uint64
	Kind       core.ResourceKind
}

// History transfers the operator's steps to operator histories.
func (o *Operator) History() []OpHistory {
	now := time.Now()
	var histories []OpHistory
	var addPeerStores, removePeerStores []uint64
	for _, step := range o.steps {
		switch s := step.(type) {
		case TransferLeader:
			histories = append(histories, OpHistory{
				FinishTime: now,
				From:       s.FromStore,
				To:         s.ToStore,
				Kind:       core.LeaderKind,
			})
		case AddPeer:
			addPeerStores = append(addPeerStores, s.ToStore)
		case AddLearner:
			addPeerStores = append(addPeerStores, s.ToStore)
		case RemovePeer:
			removePeerStores = append(removePeerStores, s.FromStore)
		}
	}
	for i := range addPeerStores {
		if i < len(removePeerStores) {
			histories = append(histories, OpHistory{
				FinishTime: now,
				From:       removePeerStores[i],
				To:         addPeerStores[i],
				Kind:       core.RegionKind,
			})
		}
	}
	return histories
}

// GetAdditionalInfo returns additional info with string
func (o *Operator) GetAdditionalInfo() string {
	if len(o.AdditionalInfos) != 0 {
		additionalInfo, err := json.Marshal(o.AdditionalInfos)
		if err == nil {
			return string(additionalInfo)
		}
	}
	return ""
}

// NewTestOperator creates a test operator.
func NewTestOperator(desc, brief string, regionID uint64, regionEpoch *metapb.RegionEpoch, kind OpKind, steps ...OpStep) *Operator {
	return NewOperator(desc, brief, regionID, regionEpoch, kind, steps...)
}
