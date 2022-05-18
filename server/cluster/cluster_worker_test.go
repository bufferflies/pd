// Copyright 2016 TiKV Project Authors.
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

package cluster

import (
	"context"
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/server/core"
	_ "github.com/tikv/pd/server/schedulers"
	"github.com/tikv/pd/server/storage"
)

var _ = Suite(&testClusterWorkerSuite{})

type testClusterWorkerSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testClusterWorkerSuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testClusterWorkerSuite) SetUpTest(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testClusterWorkerSuite) TestReportSplit(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	left := &metapb.Region{Id: 1, StartKey: []byte("a"), EndKey: []byte("b")}
	right := &metapb.Region{Id: 2, StartKey: []byte("b"), EndKey: []byte("c")}
	_, err = cluster.HandleReportSplit(&pdpb.ReportSplitRequest{Left: left, Right: right})
	c.Assert(err, IsNil)
	_, err = cluster.HandleReportSplit(&pdpb.ReportSplitRequest{Left: right, Right: left})
	c.Assert(err, NotNil)
}

func (s *testClusterWorkerSuite) TestReportBatchSplit(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	regions := []*metapb.Region{
		{Id: 1, StartKey: []byte(""), EndKey: []byte("a")},
		{Id: 2, StartKey: []byte("a"), EndKey: []byte("b")},
		{Id: 3, StartKey: []byte("b"), EndKey: []byte("c")},
		{Id: 3, StartKey: []byte("c"), EndKey: []byte("")},
	}
	_, err = cluster.HandleBatchReportSplit(&pdpb.ReportBatchSplitRequest{Regions: regions})
	c.Assert(err, IsNil)
}

func (s *testClusterWorkerSuite) TestValidateBucketRequest(c *C) {
	startKey, endKey := []byte("100"), []byte("200")
	testdate := []struct {
		keys   [][]byte
		expect [][]byte
	}{{
		[][]byte{[]byte("000"), []byte("100")},
		nil,
	}, {
		[][]byte{[]byte("200"), []byte("300")},
		nil,
	}, {
		[][]byte{[]byte("100"), []byte("200")},
		[][]byte{[]byte("100"), []byte("200")},
	}, {
		[][]byte{[]byte("120"), []byte("150"), []byte("180")},
		[][]byte{[]byte("100"), []byte("150"), []byte("200")},
	}}
	for i, v := range testdate {
		fmt.Println("test case", i)
		buckets := metapb.Buckets{Keys: v.keys}
		err := validateBucketsRequest(startKey, endKey, buckets)
		if v.expect == nil {
			c.Assert(err, NotNil)
		} else {
			c.Assert(buckets.Keys, DeepEquals, v.expect)
		}
	}
}
