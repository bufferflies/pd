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
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"testing"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testHotBucketCache{})

type testHotBucketCache struct{}

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

func newTestBuckets(regionID uint64, keys [][]byte) *metapb.Buckets {
	flow := make([]uint64, len(keys)-1)
	for i := range keys {
		if i == len(keys)-1 {
			continue
		}
		flow[i] = uint64(i)
	}
	rst := &metapb.Buckets{RegionId: regionID, Version: 1, Keys: keys, PeriodInMs: 1000,
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
	bucket1 := newTestBuckets(1, [][]byte{[]byte("10"), []byte("20")})
	bucket2 := newTestBuckets(2, [][]byte{[]byte("20"), []byte("30")})
	newItems, overlaps := cache.checkBucketsFlow(bucket1)
	c.Assert(overlaps, IsNil)
	cache.putItem(newItems, overlaps)
	newItems, overlaps = cache.checkBucketsFlow(bucket2)
	c.Assert(overlaps, IsNil)
	cache.putItem(newItems, overlaps)
	c.Assert(cache.find([]byte("10")), NotNil)
	c.Assert(cache.find([]byte("30")), IsNil)
	c.Assert(cache.getBucketsByKeyRange([]byte("10"), []byte("30")), HasLen, 2)
	c.Assert(cache.getBucketsByKeyRange([]byte("10"), []byte("20")), HasLen, 1)
	c.Assert(cache.getBucketsByKeyRange([]byte("1"), []byte("10")), HasLen, 0)
	c.Assert(cache.bucketsOfRegion, HasLen, 2)
	c.Assert(cache.tree.Len(), Equals, 2)

	// orgin bucket is |--10--|--20--|--30--|,overlaps will delete
	bucket3 := newTestBuckets(2, [][]byte{[]byte("15"), []byte("30")})
	bucket3.Version = 2
	newItems, overlaps = cache.checkBucketsFlow(bucket3)
	c.Assert(overlaps, HasLen, 2)
	cache.putItem(newItems, overlaps)
	c.Assert(cache.bucketsOfRegion, HasLen, 1)
	c.Assert(cache.tree.Len(), Equals, 1)
}

func (t *testHotBucketCache) TestInheritItem(c *C) {
	originBucketItem := convertToBucketTreeItem(newTestBuckets(1, [][]byte{[]byte("10"), []byte("20"), []byte("50"), []byte("60")}))
	originBucketItem.stats[0].hotDegree = 3
	originBucketItem.stats[1].hotDegree = 2
	originBucketItem.stats[2].hotDegree = 10

	testdata := []struct {
		buckets *metapb.Buckets
		expect  []int
	}{{
		// case1: one bucket can be inheritted by many buckets.
		buckets: newTestBuckets(1, [][]byte{[]byte("10"), []byte("15"), []byte("30"), []byte("40"), []byte("50")}),
		expect:  []int{3, 3, 2, 2},
	}, {
		// case2: the first start key is less than the endkey of old item.
		buckets: newTestBuckets(1, [][]byte{[]byte("20"), []byte("45"), []byte("50")}),
		expect:  []int{3, 2},
	}, {
		// case3: the first start key is less than the endkey of old item.
		buckets: newTestBuckets(1, [][]byte{[]byte("00"), []byte("05")}),
		expect:  []int{0},
	}, {
		// case4: newItem starKey is greater than old.
		buckets: newTestBuckets(1, [][]byte{[]byte("80"), []byte("90")}),
		expect:  []int{0},
	}}

	for _, v := range testdata {
		buckets := convertToBucketTreeItem(v.buckets)
		buckets.inheritItem([]*BucketTreeItem{originBucketItem})
		c.Assert(buckets.stats, HasLen, len(v.expect))
		for k, v := range v.expect {
			c.Assert(buckets.stats[k].hotDegree, Equals, v)
		}
	}
}
