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
package core

import (
	"bytes"
	"sync"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/btree"
)

var _ btree.Item = &regionKeyItem{}

// bucketItem is a btree item.
type regionKeyItem struct {
	ID       uint64
	startKey []byte
	endKey   []byte
}

// Less returns true if the region start key is less than the other.
func (r *regionKeyItem) Less(other btree.Item) bool {
	left := r.startKey
	right := other.(*regionKeyItem).startKey
	return bytes.Compare(left, right) < 0
}

func (r *regionKeyItem) contains(key []byte) bool {
	start, end := r.startKey, r.endKey
	return bytes.Compare(key, start) >= 0 && (len(end) == 0 || bytes.Compare(key, end) < 0)
}

// BucketsInfo for export
type BucketsInfo struct {
	mu      sync.RWMutex
	buckets map[uint64]*metapb.Buckets // regionID -> buckets
	tree    *btree.BTree               // for sort
}

// NewBucketsInfo creates new BucketsInfo.
func NewBucketsInfo() *BucketsInfo {
	return &BucketsInfo{
		buckets: make(map[uint64]*metapb.Buckets),
		tree:    btree.New(defaultBTreeDegree),
	}
}

// SetBuckets puts a buckets and the origin's bucket will be deleted.
func (b *BucketsInfo) SetBuckets(buckets *metapb.Buckets) ([]*metapb.Buckets, error) {
	count := len(buckets.Keys)
	startKey := buckets.Keys[0]
	endKey := buckets.Keys[count-1]
	if origin := b.GetByRegionID(buckets.RegionId); origin != nil && origin.Version == buckets.Version {
		// only update state if version is the same.
		// todo: stats should be merged if the new buckets is the same as the old buckets.
		b.buckets[buckets.RegionId] = buckets
		return []*metapb.Buckets{buckets}, nil
	}

	origins := b.GetByRange(startKey, endKey)
	for _, origin := range origins {
		item := regionKeyItem{ID: origin.RegionId, startKey: origin.Keys[0], endKey: origin.Keys[len(origin.Keys)-1]}
		b.tree.Delete(&item)
	}
	// todo : stats should be merged if the new buckets is the same as the old buckets.
	b.buckets[buckets.RegionId] = buckets
	b.tree.ReplaceOrInsert(&regionKeyItem{
		ID:       buckets.RegionId,
		startKey: startKey,
		endKey:   endKey,
	})
	return origins, nil
}

// GetByRange returns buckets array by key range.
func (b *BucketsInfo) GetByRange(startKey, endKey []byte) []*metapb.Buckets {
	var res []*metapb.Buckets
	b.scanRange(startKey, func(item *regionKeyItem) bool {
		if len(endKey) > 0 && bytes.Compare(item.startKey, endKey) >= 0 {
			return false
		}
		res = append(res, b.buckets[item.ID])
		return true
	})
	return res
}

// GetByRegionID returns buckets by regionID
func (b *BucketsInfo) GetByRegionID(regionID uint64) *metapb.Buckets {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.buckets[regionID]
}

func (b *BucketsInfo) find(bucket *metapb.Buckets) *regionKeyItem {
	item := &regionKeyItem{
		startKey: bucket.Keys[0],
	}
	var result *regionKeyItem
	b.mu.RLock()
	defer b.mu.RUnlock()
	b.tree.DescendLessOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionKeyItem)
		return false
	})
	if result != nil && !result.contains(item.startKey) {
		return nil
	}
	return result
}

func (b *BucketsInfo) scanRange(startKey []byte, f func(item *regionKeyItem) bool) {
	bucket := &metapb.Buckets{Keys: [][]byte{startKey}}
	startItem := b.find(bucket)
	if startItem == nil {
		startItem = &regionKeyItem{
			startKey: startKey,
		}
	}
	b.tree.AscendGreaterOrEqual(startItem, func(item btree.Item) bool {
		i := item.(*regionKeyItem)
		return f(i)
	})
}
