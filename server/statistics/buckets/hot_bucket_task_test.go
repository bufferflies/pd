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
	"strconv"
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testHotBucketTaskCache{})

type testHotBucketTaskCache struct {
	hotCache *HotBucketCache
	ctx      context.Context
	cancel   context.CancelFunc
}

func (s *testHotBucketTaskCache) SetUpSuite(_ *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.hotCache = NewBucketsCache(s.ctx)
}

func (s *testHotBucketTaskCache) TearDownTest(_ *C) {
	s.cancel()
}

func (s *testHotBucketTaskCache) TestCheckBucketsTask(c *C) {
	// case1： add bucket successfully
	buckets := newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("20"), []byte("30")})
	task := NewCheckPeerTask(buckets)
	c.Assert(s.hotCache.CheckAsync(task), IsTrue)
	time.Sleep(time.Millisecond * 10)
	c.Assert(s.hotCache.bucketsOfRegion, HasLen, 1)
	item := s.hotCache.bucketsOfRegion[uint64(1)]
	c.Assert(item, NotNil)
	c.Assert(item.stats, HasLen, 2)
	c.Assert(item.stats[0].Degree, Equals, -1)
	c.Assert(item.stats[1].Degree, Equals, -1)

	// case2: add bucket successful and the hot degree should inherit from the old one.
	buckets = newTestBuckets(2, 1, [][]byte{[]byte("20"), []byte("30")})
	task = NewCheckPeerTask(buckets)
	c.Assert(s.hotCache.CheckAsync(task), IsTrue)
	time.Sleep(time.Millisecond * 10)
	item = s.hotCache.bucketsOfRegion[uint64(2)]
	c.Assert(item.stats, HasLen, 1)
	c.Assert(item.stats[0].Degree, Equals, -2)

	// case3：add bucket successful and the hot degree should inherit from the old one.
	buckets = newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("20")})
	task = NewCheckPeerTask(buckets)
	c.Assert(s.hotCache.CheckAsync(task), IsTrue)
	time.Sleep(time.Millisecond * 10)
	item = s.hotCache.bucketsOfRegion[uint64(1)]
	c.Assert(item.stats, HasLen, 1)
	c.Assert(item.stats[0].Degree, Equals, -2)
}

func (s *testHotBucketTaskCache) TestCollectBucketStatsTask(c *C) {
	// case1： add bucket successfully
	for i := uint64(0); i < 10; i++ {
		buckets := convertToBucketTreeItem(newTestBuckets(i, 1, [][]byte{[]byte(strconv.FormatUint(i*10, 10)),
			[]byte(strconv.FormatUint((i+1)*10, 10))}))
		s.hotCache.putItem(buckets, nil)
	}

	task := NewCollectBucketStatsTask(0)
	c.Assert(s.hotCache.CheckAsync(task), IsTrue)
	stats := task.WaitRet(context.Background())
	c.Assert(stats, HasLen, 10)
	task = NewCollectBucketStatsTask(1)
	c.Assert(s.hotCache.CheckAsync(task), IsTrue)
	stats = task.WaitRet(context.Background())
	c.Assert(stats, HasLen, 0)
}
