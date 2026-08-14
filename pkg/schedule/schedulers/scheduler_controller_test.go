// Copyright 2026 TiKV Project Authors.
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

package schedulers

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
)

// TestAddSchedulerScatterRangeLimitConcurrent verifies that concurrent AddScheduler
// calls for the scatter-range scheduler cannot push the created count over the limit,
// i.e. the count-then-add sequence is atomic under Controller's lock.
func TestAddSchedulerScatterRangeLimitConcurrent(t *testing.T) {
	re := require.New(t)
	clean, _, tc, oc := prepareSchedulersTest()
	defer clean()

	ctx, cancel := context.WithCancel(context.Background())
	store := storage.NewStorageWithMemoryBackend()
	c := NewController(ctx, tc, store, oc)
	limit := maxScatterRangeSchedulerCount()

	n := limit + 20
	errList := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, err := CreateScheduler(types.ScatterRangeScheduler, oc, store,
				ConfigSliceDecoder(types.ScatterRangeScheduler, []string{"", "", fmt.Sprintf("r%d", i)}))
			if err != nil {
				errList[i] = err
				return
			}
			errList[i] = c.AddScheduler(s)
		}(i)
	}
	wg.Wait()
	cancel()
	c.Wait()

	success := 0
	for _, err := range errList {
		if err == nil {
			success++
		} else {
			re.ErrorContains(err, "too many scatter-range schedulers")
		}
	}
	re.Equal(limit, success)
	re.Len(c.schedulers, limit)
}
