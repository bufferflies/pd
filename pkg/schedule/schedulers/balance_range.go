// Copyright 2025 TiKV Project Authors.
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

package schedulers

import (
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const balanceRangeName = "balance-range-scheduler"

type balanceRangeSchedulerHandler struct {
	rd     *render.Render
	config *balanceRangeSchedulerConfig
}

func newBalanceRangeHandler(conf *balanceRangeSchedulerConfig) http.Handler {
	handler := &balanceRangeSchedulerHandler{
		config: conf,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", handler.updateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", handler.listConfig).Methods(http.MethodGet)
	return router
}

func (handler *balanceRangeSchedulerHandler) updateConfig(w http.ResponseWriter, _ *http.Request) {
	handler.rd.JSON(w, http.StatusBadRequest, "update config is not supported")
}

func (handler *balanceRangeSchedulerHandler) listConfig(w http.ResponseWriter, _ *http.Request) {
	conf := handler.config.clone()
	if err := handler.rd.JSON(w, http.StatusOK, conf); err != nil {
		log.Error("failed to marshal balance key range scheduler config", errs.ZapError(err))
	}
}

type balanceRangeSchedulerConfig struct {
	syncutil.RWMutex
	schedulerConfig
	balanceRangeSchedulerParam
}

type balanceRangeSchedulerParam struct {
	Role      string          `json:"role"`
	Engine    string          `json:"engine"`
	Timeout   time.Duration   `json:"timeout"`
	Ranges    []core.KeyRange `json:"ranges"`
	TableName string          `json:"table-name"`
}

func (conf *balanceRangeSchedulerConfig) clone() *balanceRangeSchedulerParam {
	conf.RLock()
	defer conf.RUnlock()
	ranges := make([]core.KeyRange, len(conf.Ranges))
	copy(ranges, conf.Ranges)
	return &balanceRangeSchedulerParam{
		Ranges:    ranges,
		Role:      conf.Role,
		Engine:    conf.Engine,
		Timeout:   conf.Timeout,
		TableName: conf.TableName,
	}
}

// EncodeConfig serializes the config.
func (s *balanceRangeScheduler) EncodeConfig() ([]byte, error) {
	s.conf.RLock()
	defer s.conf.RUnlock()
	return EncodeConfig(s.conf)
}

// ReloadConfig reloads the config.
func (s *balanceRangeScheduler) ReloadConfig() error {
	s.conf.Lock()
	defer s.conf.Unlock()

	newCfg := &balanceRangeSchedulerConfig{}
	if err := s.conf.load(newCfg); err != nil {
		return err
	}
	s.conf.Ranges = newCfg.Ranges
	s.conf.Timeout = newCfg.Timeout
	s.conf.Role = newCfg.Role
	s.conf.Engine = newCfg.Engine
	return nil
}

type balanceRangeScheduler struct {
	*BaseScheduler
	conf          *balanceRangeSchedulerConfig
	handler       http.Handler
	start         time.Time
	role          Role
	filters       []filter.Filter
	filterCounter *filter.Counter
}

// ServeHTTP implements the http.Handler interface.
func (s *balanceRangeScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// IsScheduleAllowed checks if the scheduler is allowed to schedule new operators.
func (s *balanceRangeScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpRange) < cluster.GetSchedulerConfig().GetRegionScheduleLimit()
	if !allowed {
		operator.IncOperatorLimitCounter(s.GetType(), operator.OpRange)
	}
	if time.Now().Sub(s.start) > s.conf.Timeout {
		allowed = false
		balanceRangeExpiredCounter.Inc()
	}
	return allowed
}

// BalanceRangeCreateOption is used to create a scheduler with an option.
type BalanceRangeCreateOption func(s *balanceRangeScheduler)

// newBalanceRangeScheduler creates a scheduler that tends to keep given peer role on
// special store balanced.
func newBalanceRangeScheduler(opController *operator.Controller, conf *balanceRangeSchedulerConfig, options ...BalanceRangeCreateOption) Scheduler {
	s := &balanceRangeScheduler{
		BaseScheduler: NewBaseScheduler(opController, types.BalanceRangeScheduler, conf),
		conf:          conf,
		handler:       newBalanceRangeHandler(conf),
	}
	for _, option := range options {
		option(s)
	}
	f := filter.NotSpecialEngines
	if conf.Engine == core.EngineTiFlash {
		f = filter.TiFlashEngineConstraint
	}
	s.filters = []filter.Filter{
		filter.NewEngineFilter(balanceRangeName, f),
	}
	s.role = newRole(s.conf.Role)

	s.filterCounter = filter.NewCounter(s.GetName())
	return s
}

// Schedule schedules the balance key range operator.
func (s *balanceRangeScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	balanceRangeCounter.Inc()
	opInfluence := s.OpController.GetOpInfluence(cluster.GetBasicCluster(), operator.WithRangeOption(s.conf.Ranges))
	plan, err := s.prepare(cluster, opInfluence)
	if err != nil {
		log.Error("failed to prepare balance key range scheduler", errs.ZapError(err))
		return nil, nil
	}

	downFilter := filter.NewRegionDownFilter()
	replicaFilter := filter.NewRegionReplicatedFilter(cluster)
	snapshotFilter := filter.NewSnapshotSendFilter(plan.stores, constant.Medium)
	pendingFilter := filter.NewRegionPendingFilter()
	baseRegionFilters := []filter.RegionFilter{downFilter, replicaFilter, snapshotFilter, pendingFilter}

	for sourceIndex, sourceStore := range plan.stores {
		plan.source = sourceStore
		plan.sourceScore = plan.score(plan.source.GetID())
		if plan.sourceScore < plan.averageScore {
			break
		}
		switch s.role {
		case Leader:
			plan.region = filter.SelectOneRegion(cluster.RandLeaderRegions(plan.sourceStoreID(), s.conf.Ranges), nil, baseRegionFilters...)
		case Learner:
			plan.region = filter.SelectOneRegion(cluster.RandLearnerRegions(plan.sourceStoreID(), s.conf.Ranges), nil, baseRegionFilters...)
		case Follower:
			plan.region = filter.SelectOneRegion(cluster.RandFollowerRegions(plan.sourceStoreID(), s.conf.Ranges), nil, baseRegionFilters...)
		}
		if plan.region == nil {
			balanceRangeNoRegionCounter.Inc()
			continue
		}
		log.Debug("select region", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", plan.region.GetID()))
		// Skip hot regions.
		if cluster.IsRegionHot(plan.region) {
			log.Debug("region is hot", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", plan.region.GetID()))
			balanceRangeHotCounter.Inc()
			continue
		}
		// Check region leader
		if plan.region.GetLeader() == nil {
			log.Warn("region have no leader", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", plan.region.GetID()))
			balanceRangeNoLeaderCounter.Inc()
			continue
		}
		plan.fit = replicaFilter.(*filter.RegionReplicatedFilter).GetFit()
		if op := s.transferPeer(plan, plan.stores[sourceIndex+1:]); op != nil {
			op.Counters = append(op.Counters, balanceRangeNewOperatorCounter)
			return []*operator.Operator{op}, nil
		}
	}

	if err != nil {
		log.Error("failed to prepare balance key range scheduler", errs.ZapError(err))
		return nil, nil

	}
	return nil, nil
}

// transferPeer selects the best store to create a new peer to replace the old peer.
func (s *balanceRangeScheduler) transferPeer(plan *balanceRangeSchedulerPlan, dstStores []*core.StoreInfo) *operator.Operator {
	excludeTargets := plan.region.GetStoreIDs()
	if s.role != Leader {
		excludeTargets = make(map[uint64]struct{})
	}
	conf := plan.GetSchedulerConfig()
	filters := []filter.Filter{
		filter.NewExcludedFilter(s.GetName(), nil, excludeTargets),
		filter.NewPlacementSafeguard(s.GetName(), conf, plan.GetBasicCluster(), plan.GetRuleManager(), plan.region, plan.source, plan.fit),
	}
	candidates := filter.NewCandidates(s.R, dstStores).FilterTarget(conf, nil, s.filterCounter, filters...)
	for i := range candidates.Stores {
		plan.target = candidates.Stores[len(candidates.Stores)-i-1]
		plan.targetScore = plan.score(plan.target.GetID())
		if plan.targetScore > plan.averageScore {
			break
		}
		regionID := plan.region.GetID()
		sourceID := plan.source.GetID()
		targetID := plan.target.GetID()
		if !plan.shouldBalance(s.GetName()) {
			continue
		}
		log.Debug("candidate store", zap.Uint64("region-id", regionID), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID))

		oldPeer := plan.region.GetStorePeer(sourceID)
		newPeer := &metapb.Peer{StoreId: plan.target.GetID(), Role: oldPeer.Role}
		op, err := operator.CreateMovePeerOperator(s.GetName(), plan, plan.region, operator.OpRange, oldPeer.GetStoreId(), newPeer)
		if err != nil {
			balanceRangeCreateOpFailCounter.Inc()
			return nil
		}
		sourceLabel := strconv.FormatUint(sourceID, 10)
		targetLabel := strconv.FormatUint(targetID, 10)
		op.FinishedCounters = append(op.FinishedCounters,
			balanceDirectionCounter.WithLabelValues(s.GetName(), sourceLabel, targetLabel),
		)
		op.SetAdditionalInfo("sourceScore", strconv.FormatInt(plan.sourceScore, 10))
		op.SetAdditionalInfo("targetScore", strconv.FormatInt(plan.targetScore, 10))
		return op
	}
	balanceRangeNoReplacementCounter.Inc()
	return nil
}

// balanceRangeSchedulerPlan is used to record the plan of balance key range scheduler.
type balanceRangeSchedulerPlan struct {
	sche.SchedulerCluster
	// stores is sorted by score desc
	stores []*core.StoreInfo
	// sourceMap records the storeID -> score
	sourceMap    map[uint64]int64
	source       *core.StoreInfo
	sourceScore  int64
	target       *core.StoreInfo
	targetScore  int64
	region       *core.RegionInfo
	fit          *placement.RegionFit
	averageScore int64
}

type storeInfo struct {
	store *core.StoreInfo
	score int64
}

func (s *balanceRangeScheduler) prepare(cluster sche.SchedulerCluster, opInfluence operator.OpInfluence) (*balanceRangeSchedulerPlan, error) {
	krs := core.NewKeyRanges(s.conf.Ranges)
	scanRegions, err := cluster.BatchScanRegions(krs)
	if err != nil {
		return nil, err
	}
	sources := filter.SelectSourceStores(cluster.GetStores(), s.filters, cluster.GetSchedulerConfig(), nil, nil)
	storeInfos := make(map[uint64]*storeInfo, len(sources))
	for _, source := range sources {
		storeInfos[source.GetID()] = &storeInfo{store: source}
	}
	totalScore := int64(0)
	for _, region := range scanRegions {
		for _, peer := range s.role.getPeers(region) {
			storeInfos[peer.GetStoreId()].score += 1
			totalScore += 1
		}
	}

	storeList := make([]*storeInfo, 0, len(storeInfos))
	for storeID, store := range storeInfos {
		if influence := opInfluence.GetStoreInfluence(storeID); influence != nil {
			store.score += s.role.getStoreInfluence(influence)
		}
		storeList = append(storeList, store)
	}
	sort.Slice(storeList, func(i, j int) bool {
		return storeList[i].score > storeList[j].score
	})
	sourceMap := make(map[uint64]int64)
	for _, store := range storeList {
		sourceMap[store.store.GetID()] = store.score
	}

	stores := make([]*core.StoreInfo, 0, len(storeList))
	for _, store := range storeList {
		stores = append(stores, store.store)
	}
	averageScore := int64(0)
	if len(storeList) != 0 {
		averageScore = totalScore / int64(len(storeList))
	}
	return &balanceRangeSchedulerPlan{
		SchedulerCluster: cluster,
		stores:           stores,
		sourceMap:        sourceMap,
		source:           nil,
		target:           nil,
		region:           nil,
		averageScore:     averageScore,
	}, nil
}

func (p *balanceRangeSchedulerPlan) sourceStoreID() uint64 {
	return p.source.GetID()
}

func (p *balanceRangeSchedulerPlan) targetStoreID() uint64 {
	return p.target.GetID()
}

func (p *balanceRangeSchedulerPlan) score(storeID uint64) int64 {
	return p.sourceMap[storeID]
}

func (p *balanceRangeSchedulerPlan) shouldBalance(scheduler string) bool {
	shouldBalance := p.sourceScore > p.targetScore
	if !shouldBalance && log.GetLevel() <= zap.DebugLevel {
		log.Debug("skip balance ",
			zap.String("scheduler", scheduler),
			zap.Uint64("region-id", p.region.GetID()),
			zap.Uint64("source-store", p.sourceStoreID()),
			zap.Uint64("target-store", p.targetStoreID()),
			zap.Int64("source-score", p.sourceScore),
			zap.Int64("target-score", p.targetScore),
			zap.Int64("average-region-size", p.averageScore),
		)
	}
	return shouldBalance
}

type Role int

const (
	Leader Role = iota
	Follower
	Learner
	Unknown
	RoleLen
)

func (r Role) String() string {
	switch r {
	case Leader:
		return "leader"
	case Follower:
		return "voter"
	case Learner:
		return "learner"
	default:
		return "unknown"
	}
}

func newRole(role string) Role {
	switch role {
	case "leader":
		return Leader
	case "follower":
		return Follower
	case "learner":
		return Learner
	default:
		return Unknown
	}
}

func (r Role) getPeers(region *core.RegionInfo) []*metapb.Peer {
	switch r {
	case Leader:
		return []*metapb.Peer{region.GetLeader()}
	case Follower:
		followers := region.GetFollowers()
		ret := make([]*metapb.Peer, 0, len(followers))
		for _, peer := range followers {
			ret = append(ret, peer)
		}
		return ret
	case Learner:
		learners := region.GetLearners()
		return learners
	default:
		return nil
	}
}

func (r Role) getStoreInfluence(influence *operator.StoreInfluence) int64 {
	switch r {
	case Leader:
		return influence.LeaderCount
	case Follower:
		return influence.RegionCount
	case Learner:
		return influence.RegionCount
	default:
		return 0
	}
}
