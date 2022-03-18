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

package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/typeutil"
	"go.uber.org/zap"
)

var (
	// default region max size is 144MB
	defaultRegionMaxSize = uint64(144)
	// default region split size is 96MB
	defaultRegionSplitSize = uint64(96)
	// default region max key is 144000
	defaultRegionMaxKey = uint64(1440000)
	// default region split key is 960000
	defaultRegionSplitKey = uint64(960000)
)

// StoreConfigManager is used to manage the store config.
type StoreConfigManager struct {
	mu     sync.RWMutex
	config *StoreConfig
}

// NewStoreConfigManager creates a new StoreConfigManager.
func NewStoreConfigManager() *StoreConfigManager {
	return &StoreConfigManager{}
}

// StoreConfig is the config of store like TiKV.
// generated by https://mholt.github.io/json-to-go/.
// nolint
type StoreConfig struct {
	Coprocessor struct {
		RegionMaxSize   string `json:"region-max-size"`
		RegionSplitSize string `json:"region-split-size"`
		RegionMaxKeys   int    `json:"region-max-keys"`
		RegionSplitKeys int    `json:"region-split-keys"`
	} `json:"coprocessor"`
}

// GetRegionMaxSize returns the max region size in MB
func (c *StoreConfig) GetRegionMaxSize() uint64 {
	if c == nil || len(c.Coprocessor.RegionMaxSize) == 0 {
		return defaultRegionMaxSize
	}
	return typeutil.ParseMBFromText(c.Coprocessor.RegionMaxSize, defaultRegionMaxSize)
}

// GetRegionSplitSize returns the region split size in MB
func (c *StoreConfig) GetRegionSplitSize() uint64 {
	if c == nil || len(c.Coprocessor.RegionSplitSize) == 0 {
		return defaultRegionSplitSize
	}
	return typeutil.ParseMBFromText(c.Coprocessor.RegionSplitSize, defaultRegionSplitSize)
}

// GetRegionSplitKeys returns the region split keys
func (c *StoreConfig) GetRegionSplitKeys() uint64 {
	if c == nil || c.Coprocessor.RegionSplitKeys == 0 {
		return defaultRegionSplitKey
	}
	return uint64(c.Coprocessor.RegionSplitKeys)
}

// GetStoreConfig returns the current store configuration.
func (m *StoreConfigManager) GetStoreConfig() *StoreConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

// GetRegionMaxKeys returns the region split keys
func (c *StoreConfig) GetRegionMaxKeys() uint64 {
	if c == nil || c.Coprocessor.RegionMaxKeys == 0 {
		return defaultRegionMaxKey
	}
	return uint64(c.Coprocessor.RegionMaxKeys)
}

// SetConfig updates the config with given config map.
func (m *StoreConfigManager) SetConfig(c *StoreConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.config = c
}

// Load Loads the store configuration.
// the lasted config will be saved to the file.
func (m *StoreConfigManager) Load(url string) error {
	resp, err := http.DefaultClient.Get(fmt.Sprintf("%s/config", url))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var cfg StoreConfig
	if err := json.Unmarshal(body, &cfg); err != nil {
		return err
	}
	log.Info("load store config", zap.Any("config", string(body)))
	m.SetConfig(&cfg)
	return nil
}
