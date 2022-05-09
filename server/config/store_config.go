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
	"reflect"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/netutil"
	"github.com/tikv/pd/pkg/slice"

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

// StoreConfig is the config of store like TiKV.
// generated by https://mholt.github.io/json-to-go/.
// nolint
type StoreConfig struct {
	Coprocessor `json:"coprocessor"`
}

// Coprocessor is the config of coprocessor.
type Coprocessor struct {
	RegionMaxSize   string `json:"region-max-size"`
	RegionSplitSize string `json:"region-split-size"`
	RegionMaxKeys   int    `json:"region-max-keys"`
	RegionSplitKeys int    `json:"region-split-keys"`
}

// String implements fmt.Stringer interface.
func (c *StoreConfig) String() string {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return "<nil>"
	}
	return string(data)
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

// GetRegionMaxKeys returns the region split keys
func (c *StoreConfig) GetRegionMaxKeys() uint64 {
	if c == nil || c.Coprocessor.RegionMaxKeys == 0 {
		return defaultRegionMaxKey
	}
	return uint64(c.Coprocessor.RegionMaxKeys)
}

// CheckRegionSize return error if the smallest region's size is less than mergeSize
func (c *StoreConfig) CheckRegionSize(size, mergeSize uint64) error {
	if size < c.GetRegionMaxSize() {
		return nil
	}

	if smallSize := size % c.GetRegionSplitSize(); smallSize <= mergeSize && smallSize != 0 {
		log.Info("region size is too small", zap.Uint64("size", size), zap.Uint64("mergeSize", mergeSize), zap.Uint64("smallSize", smallSize))
		return errs.ErrCheckerMergeAgain.FastGenByArgs("the smallest region of the split regions is less than max-merge-region-size, " +
			"it will be merged again")
	}
	return nil
}

// CheckRegionKeys return error if the smallest region's keys is less than mergeKeys
func (c *StoreConfig) CheckRegionKeys(keys, mergeKeys uint64) error {
	if keys < c.GetRegionMaxKeys() {
		return nil
	}

	if smallKeys := keys % c.GetRegionSplitKeys(); smallKeys <= mergeKeys && smallKeys > 0 {
		return errs.ErrCheckerMergeAgain.FastGenByArgs("the smallest region of the split regions is less than max-merge-region-keys")
	}
	return nil
}

// StoreConfigManager is used to manage the store config.
type StoreConfigManager struct {
	config atomic.Value
	source Source
}

// NewStoreConfigManager creates a new StoreConfigManager.
func NewStoreConfigManager(client *http.Client) *StoreConfigManager {
	schema := "http"
	if netutil.IsEnableHTTPS(client) {
		schema = "https"
	}

	manager := &StoreConfigManager{
		source: newTiKVConfigSource(schema, client),
	}
	manager.config.Store(&StoreConfig{})
	return manager
}

// NewTestStoreConfigManager creates a new StoreConfigManager for test.
func NewTestStoreConfigManager(whiteList []string) *StoreConfigManager {
	manager := &StoreConfigManager{
		source: newFakeSource(whiteList),
	}
	manager.config.Store(&StoreConfig{})
	return manager
}

// ObserveConfig is used to observe the config change.
func (m *StoreConfigManager) ObserveConfig(address string) error {
	cfg, err := m.source.GetConfig(address)
	if err != nil {
		return err
	}
	old := m.GetStoreConfig()
	if cfg != nil && !reflect.DeepEqual(cfg, old) {
		log.Info("sync the store config successful", zap.String("store-address", address), zap.String("store-config", cfg.String()))
		m.config.Store(cfg)
	}
	return nil
}

// GetStoreConfig returns the current store configuration.
func (m *StoreConfigManager) GetStoreConfig() *StoreConfig {
	if m == nil {
		return nil
	}
	config := m.config.Load()
	return config.(*StoreConfig)
}

// Source is used to get the store config.
type Source interface {
	GetConfig(statusAddress string) (*StoreConfig, error)
}

// TiKVConfigSource is used to get the store config from TiKV.
type TiKVConfigSource struct {
	schema string
	client *http.Client
}

func newTiKVConfigSource(schema string, client *http.Client) *TiKVConfigSource {
	return &TiKVConfigSource{
		schema: schema,
		client: client,
	}
}

// GetConfig returns the store config from TiKV.
func (s TiKVConfigSource) GetConfig(statusAddress string) (*StoreConfig, error) {
	url := fmt.Sprintf("%s://%s/config", s.schema, statusAddress)
	resp, err := s.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var cfg StoreConfig
	if err := json.Unmarshal(body, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// FakeSource is used to test.
type FakeSource struct {
	whiteList []string
}

func newFakeSource(whiteList []string) *FakeSource {
	return &FakeSource{
		whiteList: whiteList,
	}
}

// GetConfig returns the config.
func (f *FakeSource) GetConfig(url string) (*StoreConfig, error) {
	if !slice.Contains(f.whiteList, url) {
		return nil, fmt.Errorf("[url:%s] is not in white list", url)
	}
	config := &StoreConfig{
		Coprocessor{
			RegionMaxSize: "10MiB",
		},
	}
	return config, nil
}
