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

package pd

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/meta_storagepb"

	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/opt"
)

var _ meta_storagepb.MetaStorageClient = (*mockMetaStorageClient)(nil)
var _ meta_storagepb.MetaStorage_WatchClient = (*mockWatchClient)(nil)

// mockMetaStorageClient is a mock implementation of meta_storagepb.MetaStorageClient.
type mockMetaStorageClient struct {
	watchFn func(ctx context.Context, in *meta_storagepb.WatchRequest, opts ...grpc.CallOption) (meta_storagepb.MetaStorage_WatchClient, error)
	getFn   func(ctx context.Context, in *meta_storagepb.GetRequest, opts ...grpc.CallOption) (*meta_storagepb.GetResponse, error)
	putFn   func(ctx context.Context, in *meta_storagepb.PutRequest, opts ...grpc.CallOption) (*meta_storagepb.PutResponse, error)
}

func (m *mockMetaStorageClient) Watch(ctx context.Context, in *meta_storagepb.WatchRequest, opts ...grpc.CallOption) (meta_storagepb.MetaStorage_WatchClient, error) {
	return m.watchFn(ctx, in, opts...)
}

func (m *mockMetaStorageClient) Get(ctx context.Context, in *meta_storagepb.GetRequest, opts ...grpc.CallOption) (*meta_storagepb.GetResponse, error) {
	return m.getFn(ctx, in, opts...)
}

func (m *mockMetaStorageClient) Put(ctx context.Context, in *meta_storagepb.PutRequest, opts ...grpc.CallOption) (*meta_storagepb.PutResponse, error) {
	return m.putFn(ctx, in, opts...)
}

func (m *mockMetaStorageClient) Delete(ctx context.Context, in *meta_storagepb.DeleteRequest, opts ...grpc.CallOption) (*meta_storagepb.DeleteResponse, error) {
	return nil, nil
}

// mockWatchClient is a mock implementation of meta_storagepb.MetaStorage_WatchClient.
type mockWatchClient struct {
	grpc.ClientStream
	mu     sync.Mutex
	events []*meta_storagepb.WatchResponse
	idx    int
	closed bool
}

func (m *mockWatchClient) Recv() (*meta_storagepb.WatchResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.idx >= len(m.events) {
		return nil, context.Canceled
	}
	resp := m.events[m.idx]
	m.idx++
	return resp, nil
}

func (m *mockWatchClient) addEvent(resp *meta_storagepb.WatchResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, resp)
}

func newMockWatchClient(events ...*meta_storagepb.WatchResponse) *mockWatchClient {
	return &mockWatchClient{events: events}
}

func TestWatchWithMock(t *testing.T) {
	re := require.New(t)

	mockCli := &mockMetaStorageClient{}
	watchClient := newMockWatchClient(
		&meta_storagepb.WatchResponse{
			Header: &meta_storagepb.ResponseHeader{Revision: 1},
			Events: []*meta_storagepb.Event{
				{
					Type: meta_storagepb.Event_PUT,
					Kv:   &meta_storagepb.KeyValue{Key: []byte("k1"), Value: []byte("v1")},
				},
			},
		},
	)
	mockCli.watchFn = func(ctx context.Context, in *meta_storagepb.WatchRequest, opts ...grpc.CallOption) (meta_storagepb.MetaStorage_WatchClient, error) {
		re.Equal([]byte("test_key"), in.Key)
		return watchClient, nil
	}

	inner := &innerClient{
		keyspaceID:              constants.NullKeyspaceID,
		metaStorageCli:          mockCli,
		updateTokenConnectionCh: make(chan struct{}, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventCh, err := inner.Watch(ctx, []byte("test_key"))
	re.NoError(err)

	select {
	case events := <-eventCh:
		re.Len(events, 1)
		re.Equal(meta_storagepb.Event_PUT, events[0].Type)
		re.Equal([]byte("k1"), events[0].Kv.Key)
		re.Equal([]byte("v1"), events[0].Kv.Value)
	case <-ctx.Done():
		re.Fail("timeout waiting for watch events")
	}
}

func TestWatchWithPrefix(t *testing.T) {
	re := require.New(t)

	mockCli := &mockMetaStorageClient{}
	watchClient := newMockWatchClient(
		&meta_storagepb.WatchResponse{
			Header: &meta_storagepb.ResponseHeader{Revision: 1},
			Events: []*meta_storagepb.Event{
				{
					Type: meta_storagepb.Event_PUT,
					Kv:   &meta_storagepb.KeyValue{Key: []byte("prefix/k1"), Value: []byte("v1")},
				},
			},
		},
	)
	var capturedRangeEnd []byte
	mockCli.watchFn = func(ctx context.Context, in *meta_storagepb.WatchRequest, opts ...grpc.CallOption) (meta_storagepb.MetaStorage_WatchClient, error) {
		capturedRangeEnd = in.RangeEnd
		return watchClient, nil
	}

	inner := &innerClient{
		keyspaceID:              constants.NullKeyspaceID,
		metaStorageCli:          mockCli,
		updateTokenConnectionCh: make(chan struct{}, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventCh, err := inner.Watch(ctx, []byte("prefix/"), opt.WithPrefix())
	re.NoError(err)
	re.NotNil(capturedRangeEnd)

	select {
	case events := <-eventCh:
		re.Len(events, 1)
		re.Equal([]byte("prefix/k1"), events[0].Kv.Key)
	case <-ctx.Done():
		re.Fail("timeout waiting for watch events")
	}
}
