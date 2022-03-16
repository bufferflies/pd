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
	. "github.com/pingcap/check"
	"golang.org/x/net/context"
	"time"
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
	c.Assert(item.stats[0].hotDegree, Equals, -1)
	c.Assert(item.stats[1].hotDegree, Equals, -1)

	// case2: add bucket successful and the hotDgree should inheritted from the old one.
	buckets = newTestBuckets(2, 1, [][]byte{[]byte("20"), []byte("30")})
	task = NewCheckPeerTask(buckets)
	c.Assert(s.hotCache.CheckAsync(task), IsTrue)
	time.Sleep(time.Millisecond * 10)
	item = s.hotCache.bucketsOfRegion[uint64(2)]
	c.Assert(item.stats, HasLen, 1)
	c.Assert(item.stats[0].hotDegree, Equals, -2)

	//case3：add bucket successful and the hotDgre
	//e should inheritted from the old one.
	buckets = newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("20")})
	task = NewCheckPeerTask(buckets)
	c.Assert(s.hotCache.CheckAsync(task), IsTrue)
	time.Sleep(time.Millisecond * 10)
	item = s.hotCache.bucketsOfRegion[uint64(1)]
	c.Assert(item.stats, HasLen, 1)
	c.Assert(item.stats[0].hotDegree, Equals, -2)
}
