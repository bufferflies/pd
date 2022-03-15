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

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/btree"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/statistics"
)

const (
	// queue is the length of the channel used to send the statistics.
	queue = 20000
	// bucketBtreeDegree is the degree of the btree used to store the bucket.
	bucketBtreeDegree = 10
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
	tree               *btree.BTree               // regionId -> BucketsStats
	bucketsOfRegion    map[uint64]*BucketTreeItem // regionId -> BucketsStats
	reportIntervalSecs int
	taskQueue          chan flowBucketsItemTask
	ctx                context.Context
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
func (f *HotBucketCache) BucketStats(degree int) map[uint64][]*BucketStat {
	rst := make(map[uint64][]*BucketStat)
	for _, item := range f.bucketsOfRegion {
		stats := make([]*BucketStat, 0)
		for _, b := range item.stats {
			if b.hotDegree >= degree {
				stats = append(stats, b)
			}
		}
		rst[item.regionID] = stats
	}
	return rst
}

// putItem puts the item into the cache.
func (f *HotBucketCache) putItem(item *BucketTreeItem, overlaps []*BucketTreeItem) {
	// only update origin if the key range is same.
	if origin := f.bucketsOfRegion[item.regionID]; item.compareKeyRange(origin) {
		origin = item
		return
	}
	for _, item := range overlaps {
		delete(f.bucketsOfRegion, item.regionID)
		f.tree.Delete(item)
	}
	f.bucketsOfRegion[item.regionID] = item
	f.tree.ReplaceOrInsert(item)
}

// CheckAsync returns true if the task queue is not full.
func (f *HotBucketCache) CheckAsync(task flowBucketsItemTask) bool {
	select {
	case f.taskQueue <- task:
		return true
	default:
		return false
	}
}

func (f *HotBucketCache) updateItems() {
	for {
		select {
		case <-f.ctx.Done():
			return
		case task := <-f.taskQueue:
			task.runTask(f)
		}
	}
}

// checkBucketsFlow returns the new item tree and the overlaps.
func (f *HotBucketCache) checkBucketsFlow(buckets *metapb.Buckets) (newItem *BucketTreeItem, overlaps []*BucketTreeItem) {
	newItem = convertToBucketTreeItem(buckets)
	f.collectBucketsMetrics(newItem)
	origin := f.bucketsOfRegion[buckets.GetRegionId()]
	// origin is exist and the version is same.
	if newItem.compareKeyRange(origin) {
		overlaps = []*BucketTreeItem{origin}
	} else {
		overlaps = f.getBucketsByKeyRange(newItem.startKey, newItem.endKey)
	}
	newItem.inheritItem(overlaps)
	newItem.calculateHotDegree()
	return newItem, overlaps
}

// getBucketsByKeyRange returns the overlaps with the key range.
func (f *HotBucketCache) getBucketsByKeyRange(startKey, endKey []byte) []*BucketTreeItem {
	var res []*BucketTreeItem
	f.scanRange(startKey, func(item *BucketTreeItem) bool {
		if len(endKey) > 0 && bytes.Compare(item.startKey, endKey) >= 0 {
			return false
		}
		res = append(res, item)
		return true
	})
	return res
}

func (b *HotBucketCache) find(startKey []byte) *BucketTreeItem {
	item := &BucketTreeItem{
		startKey: startKey,
	}
	var result *BucketTreeItem
	b.tree.DescendLessOrEqual(item, func(i btree.Item) bool {
		result = i.(*BucketTreeItem)
		return false
	})
	if result != nil && !result.contains(item.startKey) {
		return nil
	}
	return result
}

func (b *HotBucketCache) scanRange(startKey []byte, f func(item *BucketTreeItem) bool) {
	startItem := b.find(startKey)
	if startItem == nil {
		startItem = &BucketTreeItem{
			startKey: startKey,
		}
	}
	b.tree.AscendGreaterOrEqual(startItem, func(item btree.Item) bool {
		i := item.(*BucketTreeItem)
		return f(i)
	})
}

// collectBucketsMetrics collects the metrics of the hot stats.
func (f *HotBucketCache) collectBucketsMetrics(stats *BucketTreeItem) {
	bucketsHeartbeatIntervalHist.Observe(float64(stats.interval))
	for i := 0; i < int(statistics.RegionStatCount); i++ {
		for _, bucket := range stats.stats {
			flowHist.Observe(float64(bucket.GetLoad(statistics.RegionStatKind(i))))
		}
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
	interval int64
	version  uint64
}

// less returns true if the start key is less than the other.
func (b *BucketTreeItem) Less(than btree.Item) bool {
	return bytes.Compare(b.startKey, than.(*BucketTreeItem).startKey) < 0
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
	return bytes.Compare(b.startKey, origin.startKey) == 0 && bytes.Compare(b.endKey, origin.endKey) == 0
}

func (b *BucketTreeItem) contains(key []byte) bool {
	return bytes.Compare(b.startKey, key) <= 0 && bytes.Compare(key, b.endKey) < 0
}

// inheritItem inherits the hot stats from the old item to the new item.
func (b *BucketTreeItem) inheritItem(origins []*BucketTreeItem) {
	// it will not inherit if the end key of the new item is less than the start key of the old item.
	// such as: new item: |----a----| |----b----|  origins item: |----c----| |----d----|
	if len(origins) == 0 || len(b.stats) == 0 || bytes.Compare(b.endKey, origins[0].startKey) < 0 {
		return
	}
	newItem := b.stats
	bucketStats := make([]*BucketStat, 0)
	for _, item := range origins {
		// it will skip if item is left of the new item.
		// such as: new item: |--c--|--d--|  origins item:|--b--|--c--|--d--|
		if bytes.Compare(b.startKey, item.endKey) < 0 {
			bucketStats = append(bucketStats, item.stats...)
		}
	}

	// p1/p2: the hot stats of the new/old index
	for p1, p2 := 0, 0; p1 < len(newItem) && p2 < len(bucketStats); {
		if newItem[p1].hotDegree <= bucketStats[p2].hotDegree {
			newItem[p1].hotDegree = bucketStats[p2].hotDegree
		}
		if bytes.Compare(newItem[p1].endKey, bucketStats[p2].endKey) > 0 {
			p2++
		} else {
			p1++
		}
	}
}

func (b *BucketTreeItem) calculateHotDegree() {
	for _, stat := range b.stats {
		hot := slice.AnyOf(stat, func(i int) bool {
			return stat.loads[i] > minHotThresholds[i]
		})
		if hot {
			stat.hotDegree++
		} else {
			stat.hotDegree--
		}
	}
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
		version:  buckets.Version,
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
