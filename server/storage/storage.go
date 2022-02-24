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

package storage

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/encryptionkm"
	"github.com/tikv/pd/server/storage/endpoint"
	"github.com/tikv/pd/server/storage/kv"
	"go.etcd.io/etcd/clientv3"
)

// Storage is the interface for the backend storage of the PD.
type Storage interface {
	// Introducing the kv.Base here is to provide
	// the basic key-value read/write ability for the Storage.
	kv.Base
	endpoint.ConfigStorage
	endpoint.MetaStorage
	endpoint.RuleStorage
	endpoint.ComponentStorage
	endpoint.ReplicationStatusStorage
	endpoint.GCSafePointStorage
}

// NewStorageWithMemoryBackend creates a new storage with memory backend.
func NewStorageWithMemoryBackend() Storage {
	return newMemoryBackend()
}

// NewStorageWithEtcdBackend creates a new storage with etcd backend.
func NewStorageWithEtcdBackend(client *clientv3.Client, rootPath string) Storage {
	return newEtcdBackend(client, rootPath)
}

// NewStorageWithLevelDBBackend creates a new storage with LevelDB backend.
func NewStorageWithLevelDBBackend(
	ctx context.Context,
	filePath string,
	ekm *encryptionkm.KeyManager,
) (Storage, error) {
	return newLevelDBBackend(ctx, filePath, ekm)
}

// TODO: support other KV storage backends like BadgerDB in the future.

type coreStorage struct {
	Storage
	regionStorage endpoint.RegionStorage
	bucketStorage endpoint.BucketStorage

	useRegionStorage int32
	regionLoaded     bool
	bucketLoaded     bool
	mu               sync.Mutex
}

// NewCoreStorage creates a new core storage with the given default and region storage.
// Usually, the defaultStorage is etcd-backend, and the regionStorage is LevelDB-backend.
// coreStorage can switch between the defaultStorage and regionStorage to read and write
// the region info, and all other storage interfaces will use the defaultStorage.
func NewCoreStorage(defaultStorage Storage, regionStorage endpoint.RegionStorage, bucketStorage endpoint.BucketStorage) Storage {
	return &coreStorage{
		Storage:       defaultStorage,
		regionStorage: regionStorage,
		bucketStorage: bucketStorage,
	}
}

// GetRegionStorage gets the internal region storage.
func (ps *coreStorage) GetRegionStorage() endpoint.RegionStorage {
	return ps.regionStorage
}

// SwitchToRegionStorage switches the region storage to regionStorage,
// after calling this, all region info will be read/saved by the internal
// regionStorage, and in most cases it's LevelDB-backend.
func (ps *coreStorage) SwitchToRegionStorage() {
	atomic.StoreInt32(&ps.useRegionStorage, 1)
}

// SwitchToDefaultStorage switches the region storage to defaultStorage,
// after calling this, all region info will be read/saved by the internal
// defaultStorage, and in most cases it's etcd-backend.
func (ps *coreStorage) SwitchToDefaultStorage() {
	atomic.StoreInt32(&ps.useRegionStorage, 0)
}

// LoadRegion loads one region from storage.
func (ps *coreStorage) LoadRegion(regionID uint64, region *metapb.Region) (ok bool, err error) {
	if atomic.LoadInt32(&ps.useRegionStorage) > 0 {
		return ps.regionStorage.LoadRegion(regionID, region)
	}
	return ps.Storage.LoadRegion(regionID, region)
}

// LoadRegions loads all regions from storage to RegionsInfo.
func (ps *coreStorage) LoadRegions(ctx context.Context, f func(region *core.RegionInfo) []*core.RegionInfo) error {
	if atomic.LoadInt32(&ps.useRegionStorage) > 0 {
		return ps.regionStorage.LoadRegions(ctx, f)
	}
	return ps.Storage.LoadRegions(ctx, f)
}

// LoadRegionsOnce loads all regions from storage to RegionsInfo. If the underlying storage is the region storage,
// it will only load once.
func (ps *coreStorage) LoadRegionsOnce(ctx context.Context, f func(region *core.RegionInfo) []*core.RegionInfo) error {
	if atomic.LoadInt32(&ps.useRegionStorage) == 0 {
		return ps.Storage.LoadRegions(ctx, f)
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if !ps.regionLoaded {
		if err := ps.regionStorage.LoadRegions(ctx, f); err != nil {
			return err
		}
		ps.regionLoaded = true
	}
	return nil
}

// SaveRegion saves one region to storage.
func (ps *coreStorage) SaveRegion(region *metapb.Region) error {
	if atomic.LoadInt32(&ps.useRegionStorage) > 0 {
		return ps.regionStorage.SaveRegion(region)
	}
	return ps.Storage.SaveRegion(region)
}

// DeleteRegion deletes one region from storage.
func (ps *coreStorage) DeleteRegion(region *metapb.Region) error {
	if atomic.LoadInt32(&ps.useRegionStorage) > 0 {
		return ps.regionStorage.DeleteRegion(region)
	}
	return ps.Storage.DeleteRegion(region)
}

// LoadBucketssOnce loads all buckets from storage to metapb.Buckets. If the underlying storage is the bucket storage,
// it will only load once.
func (ps *coreStorage) LoadBucketssOnce(ctx context.Context, f func(region *metapb.Buckets) []*metapb.Buckets) error {
	if atomic.LoadInt32(&ps.useRegionStorage) == 0 {
		return ps.Storage.LoadBuckets(ctx, f)
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if !ps.bucketLoaded {
		if err := ps.bucketStorage.LoadBuckets(ctx, f); err != nil {
			return err
		}
		ps.bucketLoaded = true
	}
	return nil
}

// LoadBucket loads the bucket from the storage.
func (ps *coreStorage) LoadBucket(regionID uint64, bucket *metapb.Buckets) (ok bool, err error) {
	if atomic.LoadInt32(&ps.useRegionStorage) == 1 {
		return ps.bucketStorage.LoadBucket(regionID, bucket)
	}
	return ps.bucketStorage.LoadBucket(regionID, bucket)
}

// LoadBuckets loads all buckets from storage to *metapb.Buckets.
func (ps *coreStorage) LoadBuckets(ctx context.Context, f func(region *metapb.Buckets) []*metapb.Buckets) error {
	if atomic.LoadInt32(&ps.useRegionStorage) > 0 {
		return ps.bucketStorage.LoadBuckets(ctx, f)
	}
	return ps.Storage.LoadBuckets(ctx, f)
}

// SaveBucket saves one bucket to storage.
func (ps *coreStorage) SaveBucket(buckets *metapb.Buckets) error {
	if atomic.LoadInt32(&ps.useRegionStorage) > 0 {
		return ps.bucketStorage.SaveBucket(buckets)
	}
	return ps.Storage.SaveBucket(buckets)
}

// DeleteRegion deletes one buckets from storage.
func (ps *coreStorage) DeleteBucket(buckets *metapb.Buckets) error {
	if atomic.LoadInt32(&ps.useRegionStorage) > 0 {
		return ps.bucketStorage.DeleteBucket(buckets)
	}
	return ps.Storage.DeleteBucket(buckets)
}

// Flush flushes the dirty region to storage.
// In coreStorage, only the regionStorage is flushed.
func (ps *coreStorage) Flush() error {
	if ps.regionStorage != nil {
		return ps.regionStorage.Flush()
	}
	return nil
}

// Close closes the region storage.
// In coreStorage, only the regionStorage is closable.
func (ps *coreStorage) Close() error {
	if ps.regionStorage != nil {
		return ps.regionStorage.Close()
	}
	return nil
}
