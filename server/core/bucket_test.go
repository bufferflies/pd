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
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
)

var _ = Suite(&testBucketSuite{})

type testBucketSuite struct{}

func (b *testBucketSuite) TestBucket(c *C) {
	bucketsInfo := NewBucketsInfo()

	// case1: add a new bucket
	bucket := &metapb.Buckets{RegionId: 1, Version: 1, Keys: [][]byte{{1}, {10}, {20}}}
	bucketsInfo.SetBuckets(bucket)
	c.Assert(bucketsInfo.GetByRegionID(1), Equals, bucket)
	c.Assert(bucketsInfo.GetByRange([]byte{1}, []byte{10}), HasLen, 1)
	c.Assert(bucketsInfo.GetByRange([]byte{1}, []byte{10})[0], Equals, bucket)

	// case2: update a bucket with a new version and new range
	// the origin bucket key range will be deleted
	bucket = &metapb.Buckets{RegionId: 1, Version: 2, Keys: [][]byte{{2}, {10}}}
	bucketsInfo.SetBuckets(bucket)
	c.Assert(bucketsInfo.GetByRegionID(1), Equals, bucket)
	c.Assert(bucketsInfo.GetByRange([]byte{1}, []byte{10}), HasLen, 1)
	c.Assert(bucketsInfo.GetByRange([]byte{10}, []byte{20}), IsNil)
}
