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
	"context"
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testHotBucketCache{})

type testHotBucketCache struct{}

func (t *testHotBucketCache) TestPutItem(c *C) {
	// case1: region split
	// origin:  |10|20|30|
	// new: 	|10|15|20|30|
	//when report bucket[15:20], the origin should be truncate into two region
	cache := NewBucketsCache(context.Background())
	testdata := []struct {
		regionID    uint64
		keys        [][]byte
		regionCount int
		treeLen     int
		version     uint64
	}{{
		regionID:    1,
		keys:        [][]byte{[]byte("10"), []byte("20"), []byte("30")},
		treeLen:     1,
		regionCount: 1,
	}, {
		regionID:    2,
		keys:        [][]byte{[]byte("15"), []byte("20")},
		regionCount: 1,
		treeLen:     3,
	}, {
		regionID:    1,
		keys:        [][]byte{[]byte("20"), []byte("30")},
		version:     2,
		regionCount: 2,
		treeLen:     3,
	}, {
		regionID:    3,
		keys:        [][]byte{[]byte("10"), []byte("15")},
		regionCount: 3,
		treeLen:     3,
	}, {
		// region 1,2,3 will be merged.
		regionID:    4,
		keys:        [][]byte{[]byte("10"), []byte("30")},
		regionCount: 1,
		treeLen:     1,
	}}
	for i, v := range testdata {
		fmt.Println(i)
		bucket := convertToBucketTreeItem(newTestBuckets(v.regionID, v.version, v.keys))
		origins := cache.getBucketsByKeyRange(bucket.startKey, bucket.endKey)
		cache.putItem(bucket, origins)
		c.Assert(cache.bucketsOfRegion, HasLen, v.regionCount)
		c.Assert(cache.tree.Len(), Equals, v.treeLen)
		c.Assert(cache.bucketsOfRegion[v.regionID], NotNil)
		c.Assert(cache.find([]byte("10")), NotNil)
		if v.treeLen != v.regionCount {
			c.Assert(cache.find([]byte("10")).status, Equals, archive)
		}
	}

}

//func (t *testHotPeerCache) TestCache(c *C) {
func (t *testHotBucketCache) TestConvertToBucketTreeStat(c *C) {
	buckets := &metapb.Buckets{
		RegionId: 1,
		Version:  0,
		Keys:     [][]byte{{'1'}, {'2'}, {'3'}, {'4'}, {'5'}},
		Stats: &metapb.BucketStats{
			ReadBytes:  []uint64{1, 2, 3, 4},
			ReadKeys:   []uint64{1, 2, 3, 4},
			ReadQps:    []uint64{1, 2, 3, 4},
			WriteBytes: []uint64{1, 2, 3, 4},
			WriteKeys:  []uint64{1, 2, 3, 4},
			WriteQps:   []uint64{1, 2, 3, 4},
		},
		PeriodInMs: 1000,
	}
	item := convertToBucketTreeItem(buckets)
	c.Assert(item.startKey, BytesEquals, []byte{'1'})
	c.Assert(item.endKey, BytesEquals, []byte{'5'})
	c.Assert(item.regionID, Equals, uint64(1))
	c.Assert(item.version, Equals, uint64(0))
	c.Assert(item.stats, HasLen, 4)
}

func newTestBuckets(regionID uint64, version uint64, keys [][]byte) *metapb.Buckets {
	flow := make([]uint64, len(keys)-1)
	for i := range keys {
		if i == len(keys)-1 {
			continue
		}
		flow[i] = uint64(i)
	}
	rst := &metapb.Buckets{RegionId: regionID, Version: version, Keys: keys, PeriodInMs: 1000,
		Stats: &metapb.BucketStats{
			ReadBytes:  flow,
			ReadKeys:   flow,
			ReadQps:    flow,
			WriteBytes: flow,
			WriteKeys:  flow,
			WriteQps:   flow,
		}}
	return rst

}

func (t *testHotBucketCache) TestGetBucketsByKeyRange(c *C) {
	cache := NewBucketsCache(context.Background())
	bucket1 := newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("15")})
	bucket2 := newTestBuckets(2, 1, [][]byte{[]byte("15"), []byte("20")})
	bucket3 := newTestBuckets(3, 1, [][]byte{[]byte("20"), []byte("30")})
	newItems, overlaps := cache.checkBucketsFlow(bucket1)
	c.Assert(overlaps, IsNil)
	cache.putItem(newItems, overlaps)
	newItems, overlaps = cache.checkBucketsFlow(bucket2)
	c.Assert(overlaps, IsNil)
	cache.putItem(newItems, overlaps)
	newItems, overlaps = cache.checkBucketsFlow(bucket3)
	c.Assert(overlaps, IsNil)
	cache.putItem(newItems, overlaps)
	c.Assert(cache.find([]byte("10")), NotNil)
	c.Assert(cache.find([]byte("30")), IsNil)
	c.Assert(cache.getBucketsByKeyRange([]byte("10"), []byte("30")), HasLen, 3)
	c.Assert(cache.getBucketsByKeyRange([]byte("10"), []byte("20")), HasLen, 2)
	c.Assert(cache.getBucketsByKeyRange([]byte("1"), []byte("10")), HasLen, 0)
	c.Assert(cache.bucketsOfRegion, HasLen, 3)
	c.Assert(cache.tree.Len(), Equals, 3)

}

func (t *testHotBucketCache) TestInherit(c *C) {
	originBucketItem := convertToBucketTreeItem(newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("20"), []byte("50"), []byte("60")}))
	originBucketItem.stats[0].hotDegree = 3
	originBucketItem.stats[1].hotDegree = 2
	originBucketItem.stats[2].hotDegree = 10

	testdata := []struct {
		buckets *metapb.Buckets
		expect  []int
	}{{
		// case1: one bucket can be inheritted by many buckets.
		buckets: newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("20"), []byte("30"), []byte("40"), []byte("50")}),
		expect:  []int{3, 2, 2, 2},
	}, {
		// case2: the first start key is less than the end key of old item.
		buckets: newTestBuckets(1, 1, [][]byte{[]byte("20"), []byte("45"), []byte("50")}),
		expect:  []int{2, 2},
	}, {
		// case3: the first start key is less than the end key of old item.
		buckets: newTestBuckets(1, 1, [][]byte{[]byte("00"), []byte("05")}),
		expect:  []int{0},
	}, {
		// case4: newItem starKey is greater than old.
		buckets: newTestBuckets(1, 1, [][]byte{[]byte("80"), []byte("90")}),
		expect:  []int{0},
	}}

	for _, v := range testdata {
		buckets := convertToBucketTreeItem(v.buckets)
		buckets.inherit([]*BucketTreeItem{originBucketItem})
		c.Assert(buckets.stats, HasLen, len(v.expect))
		for k, v := range v.expect {
			c.Assert(buckets.stats[k].hotDegree, Equals, v)
		}
	}
}

func (t *testHotBucketCache) TestSplit(c *C) {
	testdata := []struct {
		splitKey []byte
		dir      direction
		success  bool
	}{{
		splitKey: []byte("10"),
		dir:      left,
		success:  true,
	}, {
		splitKey: []byte("10"),
		dir:      right,
		success:  true,
	}, {
		splitKey: []byte("20"),
		dir:      left,
		success:  true,
	}, {
		splitKey: []byte("20"),
		dir:      right,
		success:  true,
	}, {
		splitKey: []byte("05"),
		dir:      left,
		success:  false,
	}, {
		splitKey: []byte("05"),
		dir:      right,
		success:  false,
	}, {
		splitKey: []byte("60"),
		dir:      left,
		success:  true,
	}, {
		splitKey: []byte("60"),
		dir:      right,
		success:  true,
	}}
	for i, v := range testdata {
		fmt.Println(i)
		origin := convertToBucketTreeItem(newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("20"), []byte("60")}))
		c.Assert(origin.split(v.splitKey, v.dir), Equals, v.success)
		if !v.success {
			continue
		}
		if v.dir == left {
			c.Assert(origin.endKey, BytesEquals, v.splitKey)
			c.Assert(origin.stats[len(origin.stats)-1].endKey, BytesEquals, v.splitKey)
		} else {
			c.Assert(origin.startKey, BytesEquals, v.splitKey)
			c.Assert(origin.stats[0].startKey, BytesEquals, v.splitKey)
		}
	}
}

func (t *testHotBucketCache) TestClip(c *C) {
	origins := []*BucketTreeItem{
		convertToBucketTreeItem(newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("20"), []byte("60")})),
		convertToBucketTreeItem(newTestBuckets(2, 1, [][]byte{[]byte("80"), []byte("100")})),
	}

	testdata := []struct {
		buckets  *metapb.Buckets
		count    int
		startKey []byte
	}{{
		//
		buckets:  newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("20")}),
		startKey: []byte("10"),
		count:    3,
	}, {
		buckets:  newTestBuckets(1, 1, [][]byte{[]byte("20"), []byte("50")}),
		startKey: []byte("20"),
		count:    2,
	}, {
		// case3: the first buckets doesn't contain the start key
		buckets:  newTestBuckets(1, 1, [][]byte{[]byte("80"), []byte("100")}),
		startKey: []byte("50"),
		count:    0,
	}}
	for _, v := range testdata {
		item := convertToBucketTreeItem(v.buckets)
		stats := item.clip(origins)
		c.Assert(stats, HasLen, v.count)
		if v.count > 0 {
			c.Assert(stats[0].startKey, BytesEquals, v.startKey)
		}
	}
}
