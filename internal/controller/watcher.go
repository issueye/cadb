package controller

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/metadata"
	"golang.corp.yxkj.com/orange/cadb/internal/global"
	"golang.corp.yxkj.com/orange/cadb/internal/grpc/proto/proto"
	"golang.corp.yxkj.com/orange/cadb/internal/middleware"
	"golang.corp.yxkj.com/orange/cadb/internal/store"
)

type Watcher struct{}

// WatchLock
// 观察锁
func (Watcher *Watcher) WatchLock(req *proto.KeyRequest, stream proto.CadbWatchHelper_WatchLockServer) error {
	value := metadata.ExtractIncoming(stream.Context()).Get(middleware.AuthKey)
	watcher := store.WatchLock(value, req.Key, func(WT store.WType, entry *store.Notification) {
		t := proto.WatchType(WT)

		stream.Send(&proto.KeyChange{
			Type: t,
			Key:  entry.Key,
			Data: &proto.Notification{
				Key:     entry.Key,
				Value:   entry.Entry.Value,
				IsLock:  entry.Entry.IsLock,
				LeaseID: entry.Entry.LeaseID,
			},
		})
	})

	// 阻塞
	select {
	case <-stream.Context().Done():
		{
			global.Log.Infof("通过终止会话[%s]结束会话 key: %s", value, req.Key)
			store.RemoveWatch(req.Key, value)
		}
	case <-watcher.Close:
		global.Log.Infof("通过CloseWatch[%s]结束会话 key: %s", value, req.Key)
	}
	return nil
}

// CloseWatchLock
// 关闭观察锁
func (Watcher *Watcher) CloseWatchLock(ctx context.Context, req *proto.KeyRequest) (*proto.Empty, error) {
	value := metadata.ExtractIncoming(ctx).Get(middleware.AuthKey)
	store.RemoveWatchLock(req.Key, value)
	return &proto.Empty{}, nil
}

// Watch
// 观察
func (Watcher *Watcher) Watch(req *proto.KeyRequest, stream proto.CadbWatchHelper_WatchServer) error {
	value := metadata.ExtractIncoming(stream.Context()).Get(middleware.AuthKey)
	watcher := store.Watch(value, req.Key, func(WT store.WType, entry *store.Notification) {
		t := proto.WatchType(WT)

		stream.Send(&proto.KeyChange{
			Type: t,
			Key:  entry.Key,
			Data: &proto.Notification{
				Key:     entry.Key,
				Value:   entry.Entry.Value,
				IsLock:  entry.Entry.IsLock,
				LeaseID: entry.Entry.LeaseID,
			},
		})
	})

	// 阻塞
	select {
	case <-stream.Context().Done():
		{
			global.Log.Infof("通过终止会话[%s]结束会话 key: %s", value, req.Key)
			store.RemoveWatch(req.Key, value)
		}
	case <-watcher.Close:
		global.Log.Infof("通过CloseWatch[%s]结束会话 key: %s", value, req.Key)
	}
	return nil
}

// CloseWatch
// 关闭观察
func (Watcher *Watcher) CloseWatch(ctx context.Context, req *proto.KeyRequest) (*proto.Empty, error) {
	value := metadata.ExtractIncoming(ctx).Get(middleware.AuthKey)
	store.RemoveWatch(req.Key, value)
	return &proto.Empty{}, nil
}
