// Copyright 2022 TiKV Project Authors.
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

package buckets

import (
	"bytes"
	"context"
	"fmt"
	"github.com/tikv/pd/pkg/logutil"
	"go.uber.org/zap"
	"sort"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/btree"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/statistics"
)

type direction int

const (
	left direction = iota
	right
)

type status int

const (
	alive status = iota
	archive
)

const (
	// queue is the length of the channel used to send the statistics.
	queue = 20000
	// bucketBtreeDegree is the degree of the btree used to store the bucket.
	bucketBtreeDegree = 10

	// the range of the hot degree should be [-1000, 10000]
	minHotDegree = -100
	maxHotDegree = 100
)

var minHotThresholds = [statistics.RegionStatCount]uint64{
	statistics.RegionWriteBytes: 1 * 1024,
	statistics.RegionWriteKeys:  32,
	statistics.RegionWriteQuery: 32,
	statistics.RegionReadBytes:  8 * 1024,
	statistics.RegionReadKeys:   128,
	statistics.RegionReadQuery:  128,
}

// HotBucketCache is the cache of hot stats.
type HotBucketCache struct {
	tree            *btree.BTree               // regionId -> BucketsStats
	bucketsOfRegion map[uint64]*BucketTreeItem // regionId -> BucketsStats
	taskQueue       chan flowBucketsItemTask
	ctx             context.Context
}

// NewBucketsCache creates a new hot spot cache.
func NewBucketsCache(ctx context.Context) *HotBucketCache {
	cache := &HotBucketCache{
		ctx:             ctx,
		bucketsOfRegion: make(map[uint64]*BucketTreeItem),
		tree:            btree.New(bucketBtreeDegree),
		taskQueue:       make(chan flowBucketsItemTask, queue),
	}
	go cache.updateItems()
	return cache
}

// BucketStats returns the hot stats of the regions that great than degree.
func (h *HotBucketCache) BucketStats(degree int) map[uint64][]*BucketStat {
	rst := make(map[uint64][]*BucketStat)
	for _, item := range h.bucketsOfRegion {
		stats := make([]*BucketStat, 0)
		for _, b := range item.stats {
			if b.hotDegree >= degree {
				stats = append(stats, b)
			}
		}
		if len(stats) > 0 {
			rst[item.regionID] = stats
		}
	}
	return rst
}

// putItem puts the item into the cache.
func (h *HotBucketCache) putItem(item *BucketTreeItem, overlaps []*BucketTreeItem) {
	// only update origin if the key range is same.
	if origin := h.bucketsOfRegion[item.regionID]; item.compareKeyRange(origin) {
		*origin = *item
		return
	}
	log.Info("putItem", zap.Stringer("item", item), zap.Int("overlaps", len(overlaps)), zap.Uint64("regionID", item.regionID))
	// the origin bucket split two region(a,b) if overlaps is one and bigger than the new.
	// b will be inserted into the tree
	if len(overlaps) == 1 && overlaps[0].bigger(item) {
		extra := overlaps[0].Clone()
		extra.split(item.endKey, right)
		h.tree.ReplaceOrInsert(extra)
		overlaps = append(overlaps, extra)
		delete(h.bucketsOfRegion, overlaps[0].regionID)
	}
	// if the key range is not same, we need to remove the old item and add the new one.
	for i, overlap := range overlaps {
		if overlap.status == alive {
			delete(h.bucketsOfRegion, overlap.regionID)
		}
		if i == 0 {
			overlap.split(item.startKey, left)
			overlap.status = archive
		} else if i == len(overlaps)-1 {
			overlap.split(item.endKey, right)
			overlap.status = archive
		} else {
			log.Info("delete middle bucket from tree", zap.ByteString("start-key", overlap.startKey), zap.ByteString("end-key", overlap.endKey))
			h.tree.Delete(overlap)
		}
		if bytes.Compare(overlap.startKey, overlap.endKey) >= 0 {
			log.Info("delete borderless bucket from tree", zap.ByteString("keys", overlap.startKey))
			h.tree.Delete(overlap)
		}
	}
	h.bucketsOfRegion[item.regionID] = item
	h.tree.ReplaceOrInsert(item)
}

// CheckAsync returns true if the task queue is not full.
func (h *HotBucketCache) CheckAsync(task flowBucketsItemTask) bool {
	select {
	case h.taskQueue <- task:
		return true
	default:
		return false
	}
}

func (h *HotBucketCache) updateItems() {
	defer logutil.LogPanic()
	for {
		select {
		case <-h.ctx.Done():
			return
		case task := <-h.taskQueue:
			task.runTask(h)
		}
	}
}

// checkBucketsFlow returns the new item tree and the overlaps.
func (h *HotBucketCache) checkBucketsFlow(buckets *metapb.Buckets) (newItem *BucketTreeItem, overlaps []*BucketTreeItem) {
	newItem = convertToBucketTreeItem(buckets)
	// origin is existed and the version is same.
	if origin := h.bucketsOfRegion[buckets.GetRegionId()]; newItem.compareKeyRange(origin) {
		overlaps = []*BucketTreeItem{origin}
	} else {
		overlaps = h.getBucketsByKeyRange(newItem.startKey, newItem.endKey)
	}
	newItem.inherit(overlaps)
	newItem.calculateHotDegree()
	h.collectBucketsMetrics(newItem)
	return newItem, overlaps
}

// getBucketsByKeyRange returns the overlaps with the key range.
func (h *HotBucketCache) getBucketsByKeyRange(startKey, endKey []byte) []*BucketTreeItem {
	var res []*BucketTreeItem
	h.scanRange(startKey, func(item *BucketTreeItem) bool {
		if len(endKey) > 0 && bytes.Compare(item.startKey, endKey) >= 0 {
			return false
		}
		res = append(res, item)
		return true
	})
	return res
}

func (h *HotBucketCache) find(startKey []byte) *BucketTreeItem {
	item := &BucketTreeItem{
		startKey: startKey,
	}
	var result *BucketTreeItem
	h.tree.DescendLessOrEqual(item, func(i btree.Item) bool {
		result = i.(*BucketTreeItem)
		return false
	})
	if result != nil && !result.contains(item.startKey) {
		return nil
	}
	return result
}

func (h *HotBucketCache) scanRange(startKey []byte, f func(item *BucketTreeItem) bool) {
	startItem := h.find(startKey)
	if startItem == nil {
		startItem = &BucketTreeItem{
			startKey: startKey,
		}
	}
	h.tree.AscendGreaterOrEqual(startItem, func(item btree.Item) bool {
		i := item.(*BucketTreeItem)
		return f(i)
	})
}

// collectBucketsMetrics collects the metrics of the hot stats.
func (h *HotBucketCache) collectBucketsMetrics(stats *BucketTreeItem) {
	bucketsHeartbeatIntervalHist.Observe(float64(stats.interval))
	for _, bucket := range stats.stats {
		BucketsHotDegreeHist.Observe(float64(bucket.hotDegree))
	}
}

// BucketStat is the record the bucket statistics.
type BucketStat struct {
	regionID  uint64
	startKey  []byte
	endKey    []byte
	hotDegree int
	interval  uint64
	// see statistics.RegionStatKind
	loads []uint64
}

// GetLoad returns the given kind load of the bucket.
func (stat *BucketStat) GetLoad(kind statistics.RegionStatKind) uint64 {
	return stat.loads[kind]
}

// BucketTreeItem is the item of the bucket btree.
type BucketTreeItem struct {
	regionID uint64
	startKey []byte
	endKey   []byte
	stats    []*BucketStat
	interval uint64
	version  uint64
	status   status
}

// Less returns true if the start key is less than the other.
func (b *BucketTreeItem) Less(than btree.Item) bool {
	return bytes.Compare(b.startKey, than.(*BucketTreeItem).startKey) < 0
}

func (b *BucketTreeItem) bigger(other *BucketTreeItem) bool {
	return bytes.Compare(b.startKey, other.startKey) < 0 &&
		bytes.Compare(b.endKey, other.endKey) > 0
}

// compareKeyRange returns whether the key range is overlaps with the item.
func (b *BucketTreeItem) compareKeyRange(origin *BucketTreeItem) bool {
	if origin == nil {
		return false
	}
	// key range must be same if the version is same.
	if b.version == origin.version {
		return true
	}
	return bytes.Equal(b.startKey, origin.startKey) && bytes.Equal(b.endKey, origin.endKey)
}

func (b *BucketTreeItem) Clone() *BucketTreeItem {
	item := &BucketTreeItem{
		regionID: b.regionID,
		startKey: b.startKey,
		endKey:   b.endKey,
		interval: b.interval,
		version:  b.version,
		stats:    make([]*BucketStat, len(b.stats)),
	}
	copy(item.stats, b.stats)
	return item
}

func (b *BucketTreeItem) contains(key []byte) bool {
	return bytes.Compare(b.startKey, key) <= 0 && bytes.Compare(key, b.endKey) < 0
}

// inherit the hot stats from the old item to the new item.
func (b *BucketTreeItem) inherit(origins []*BucketTreeItem) {
	// it will not inherit if the end key of the new item is less than the start key of the old item.
	// such as: new item: |----a----| |----b----|
	// origins item: |----c----| |----d----|
	if len(origins) == 0 || len(b.stats) == 0 || bytes.Compare(b.endKey, origins[0].startKey) < 0 {
		return
	}
	newItem := b.stats
	bucketStats := b.clip(origins)

	// p1/p2: the hot stats of the new/old index
	for p1, p2 := 0, 0; p1 < len(newItem) && p2 < len(bucketStats); {
		oldDegree := bucketStats[p2].hotDegree
		newDegree := newItem[p1].hotDegree
		if oldDegree < 0 && newDegree <= 0 && oldDegree < newDegree {
			newItem[p1].hotDegree = oldDegree
		}
		if oldDegree > 0 && oldDegree > newDegree {
			newItem[p1].hotDegree = oldDegree
		}
		if bytes.Compare(newItem[p1].endKey, bucketStats[p2].endKey) > 0 {
			p2++
		} else if bytes.Equal(newItem[p1].endKey, bucketStats[p2].endKey) {
			p2++
			p1++
		} else {
			p1++
		}
	}
}

// clip clips origins bucket to BucketStat array
func (b *BucketTreeItem) clip(origins []*BucketTreeItem) []*BucketStat {
	// the first buckets should contains the start key.
	if len(origins) == 0 || !origins[0].contains(b.startKey) {
		return nil
	}
	bucketStats := make([]*BucketStat, 0)
	index := sort.Search(len(origins[0].stats), func(i int) bool {
		return bytes.Compare(b.startKey, origins[0].stats[i].endKey) < 0
	})
	bucketStats = append(bucketStats, origins[0].stats[index:]...)
	for i := 1; i < len(origins); i++ {
		bucketStats = append(bucketStats, origins[i].stats...)
	}
	return bucketStats
}

func (b *BucketTreeItem) split(key []byte, dir direction) bool {
	if !b.contains(key) && !bytes.Equal(b.endKey, key) {
		return false
	}
	if dir == left {
		b.endKey = key
		index := sort.Search(len(b.stats), func(i int) bool {
			return bytes.Compare(b.stats[i].endKey, key) >= 0
		})
		b.stats = b.stats[:index+1]
		b.stats[len(b.stats)-1].endKey = key
	} else {
		b.startKey = key
		index := sort.Search(len(b.stats), func(i int) bool {
			return bytes.Compare(b.stats[i].startKey, key) > 0
		})
		b.stats = b.stats[index-1:]
		b.stats[0].startKey = key
	}
	return true
}

func (b *BucketTreeItem) calculateHotDegree() {
	for _, stat := range b.stats {
		hot := slice.AnyOf(stat.loads, func(i int) bool {
			return stat.loads[i] > minHotThresholds[i]
		})
		if hot && stat.hotDegree < maxHotDegree {
			stat.hotDegree++
		}
		if !hot && stat.hotDegree > minHotDegree {
			stat.hotDegree--
		}
	}
}

func (b *BucketTreeItem) String() string {
	return fmt.Sprintf("[region-id:%d][start-key:%s][end-key:%s][status:%v]", b.regionID, b.startKey, b.endKey, b.status)
}

// convertToBucketTreeItem
func convertToBucketTreeItem(buckets *metapb.Buckets) *BucketTreeItem {
	items := make([]*BucketStat, len(buckets.Keys)-1)
	interval := buckets.PeriodInMs / 1000
	for i := 0; i < len(buckets.Keys)-1; i++ {
		loads := []uint64{
			buckets.Stats.ReadBytes[i] / interval,
			buckets.Stats.ReadKeys[i] / interval,
			buckets.Stats.ReadQps[i] / interval,
			buckets.Stats.WriteBytes[i] / interval,
			buckets.Stats.WriteKeys[i] / interval,
			buckets.Stats.WriteQps[i] / interval,
		}
		items[i] = &BucketStat{
			regionID:  buckets.RegionId,
			startKey:  buckets.Keys[i],
			endKey:    buckets.Keys[i+1],
			hotDegree: 0,
			loads:     loads,
			interval:  interval,
		}
	}
	return &BucketTreeItem{
		startKey: getStartKey(buckets),
		endKey:   getEndKey(buckets),
		regionID: buckets.RegionId,
		stats:    items,
		interval: buckets.GetPeriodInMs() / 1000,
		version:  buckets.Version,
		status:   alive,
	}
}

func getEndKey(buckets *metapb.Buckets) []byte {
	if len(buckets.GetKeys()) == 0 {
		return nil
	}
	return buckets.Keys[len(buckets.Keys)-1]
}

func getStartKey(buckets *metapb.Buckets) []byte {
	if len(buckets.GetKeys()) == 0 {
		return nil
	}
	return buckets.Keys[0]
}
