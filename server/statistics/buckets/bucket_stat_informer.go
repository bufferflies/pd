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
	"fmt"
	
	"github.com/tikv/pd/server/core"
)

// BucketStatInformer is used to get the bucket statistics.
type BucketStatInformer interface {
	BucketsStats(degree int) map[uint64][]*BucketStat
}

// BucketStat is the record the bucket statistics.
type BucketStat struct {
	RegionID  uint64
	StartKey  []byte
	EndKey    []byte
	HotDegree int
	Interval  uint64
	// see statistics.RegionStatKind
	Loads []uint64
}

// String implement for Stringer
func (b *BucketStat) String() string {
	return fmt.Sprintf("[region-id:%d][start-key:%s][end-key-key:%s][hot-degree:%d][Interval:%d(ms)][Loads:%v]",
		b.RegionID, core.HexRegionKeyStr(b.StartKey), core.HexRegionKeyStr(b.EndKey), b.HotDegree, b.Interval, b.Loads)
}

func (b *BucketStat) clone() *BucketStat {
	c := &BucketStat{
		StartKey:  b.StartKey,
		EndKey:    b.EndKey,
		RegionID:  b.RegionID,
		HotDegree: b.HotDegree,
		Interval:  b.Interval,
		Loads:     make([]uint64, len(b.Loads)),
	}
	copy(c.Loads, b.Loads)
	return c
}
