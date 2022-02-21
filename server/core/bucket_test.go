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
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"sort"
)

var _ = Suite(&testBucketSuite{})

type testBucketSuite struct{}

func (b *testBucketSuite) TestBucket(c *C) {
	arr := []*metapb.Buckets{
		{Keys: [][]byte{[]byte("b"), []byte("c")}},
		{Keys: [][]byte{[]byte("c"), []byte("e")}},
		{Keys: [][]byte{[]byte("e"), []byte("g")}},
		{Keys: [][]byte{[]byte("g"), []byte("i")}},
		{Keys: [][]byte{[]byte("i"), []byte("j")}},
		{Keys: [][]byte{[]byte("k"), []byte("m")}},
	}
	findStart := func(key []byte) int {
		idx := sort.Search(len(arr), func(i int) bool {
			return bytes.Compare(arr[i].Keys[0], key) >= 0
		}) - 1
		if idx < 0 {
			idx = 0
		}
		return idx
	}
	findEnd := func(key []byte) int {
		idx := sort.Search(len(arr), func(i int) bool {
			return bytes.Compare(arr[i].Keys[1], key) > 0
		})
		if idx > len(arr)-1 {
			idx = len(arr) - 1
		}
		return idx
	}
	c.Assert(findStart([]byte("a")), Equals, 0)
	//c.Assert(findStart([]byte("b")), Equals, 0)
	//c.Assert(findStart([]byte("d")), Equals, 1)
	//c.Assert(findStart([]byte("o")), Equals, 5)

	c.Assert(findEnd([]byte("a")), Equals, 0)
	c.Assert(findEnd([]byte("c")), Equals, 1)
	c.Assert(findEnd([]byte("d")), Equals, 1)
	c.Assert(findEnd([]byte("o")), Equals, 5)

}

func (b *testBucketSuite) TestClip(c *C) {
	buckets := []*metapb.Buckets{
		{Keys: [][]byte{[]byte("b"), []byte("c")}},
		{Keys: [][]byte{[]byte("c"), []byte("e")}},
		{Keys: [][]byte{[]byte("e"), []byte("m")}},
	}

	testdata := []struct {
		startKey    []byte
		endKey      []byte
		BucketsInfo []*metapb.Buckets
	}{{
		startKey:    []byte("a"),
		endKey:      []byte("b"),
		BucketsInfo: buckets[:1],
	}, {
		startKey:    []byte("a"),
		endKey:      []byte("c"),
		BucketsInfo: buckets[:2],
	}, {
		startKey:    []byte("a"),
		endKey:      []byte("o"),
		BucketsInfo: buckets,
	}}
	for i, v := range testdata {
		fmt.Printf("%d: %v\n", i, v)
		rst := clip(v.startKey, v.endKey, buckets)
		c.Assert(len(rst), Equals, len(v.BucketsInfo))
		c.Assert(rst[0].Keys[0], BytesEquals, v.BucketsInfo[0].Keys[0])
	}
}
