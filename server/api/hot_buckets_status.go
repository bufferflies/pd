// Copyright 2017 TiKV Project Authors.
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

package api

import (
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/statistics/buckets"
	"github.com/unrolled/render"
	"net/http"
	"strconv"
)

type hotBucketsStatusHandler struct {
	*server.Handler
	rd *render.Render
}

func newHotBucketsStatusHandler(handler *server.Handler, rd *render.Render) *hotBucketsStatusHandler {
	return &hotBucketsStatusHandler{
		Handler: handler,
		rd:      rd,
	}
}

type hotBucketsStatus struct {
	RegionID uint64 `json:"region_id"`
	Degree   int    `json:"degree"`
	StartKey string `json:"start_key"`
	EndKey   string `json:"end_key"`
}

func convertHotBucketsStatus(stats map[uint64][]*buckets.BucketStat) map[uint64][]*hotBucketsStatus {
	rst := make(map[uint64][]*hotBucketsStatus, len(stats))
	for regionID, stat := range stats {
		rst[regionID] = make([]*hotBucketsStatus, len(stat))
		for i, bucket := range stat {
			rst[regionID][i] = &hotBucketsStatus{
				RegionID: regionID,
				Degree:   bucket.Degree,
				StartKey: core.HexRegionKeyStr(bucket.StartKey),
				EndKey:   core.HexRegionKeyStr(bucket.EndKey),
			}
		}
	}
	return rst
}

// @Tags buckets
// @Summary List the buckets.
// @Produce json
// @Success 200 {object} buckets.BucketStat
// @Router /hotspot/buckets [get]
func (h *hotBucketsStatusHandler) GetHotBuckets(w http.ResponseWriter, r *http.Request) {
	rc, err := h.GetRaftCluster()
	if rc == nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	degree := rc.GetOpts().GetHotRegionCacheHitsThreshold()
	if hot, err := strconv.Atoi(r.URL.Query().Get("degree")); err != nil {
		degree = hot
	}

	h.rd.JSON(w, http.StatusOK, convertHotBucketsStatus(rc.BucketStats(degree)))
}
