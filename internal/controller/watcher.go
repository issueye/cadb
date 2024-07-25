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

func (Watcher *Watcher) convert(entry *store.Notification) *proto.Notification {
	return &proto.Notification{
		Key:      entry.Key,
		Expire:   entry.Entry.Expire,
		Value:    entry.Entry.Value,
		IsLock:   entry.Entry.IsLock,
		LeaseID:  entry.Entry.LeaseID,
		ExpireAt: entry.Entry.ExpireAt.Format("2006-01-02 15:04:05"),
		CreateAt: entry.Entry.CreateAt.Format("2006-01-02 15:04:05"),
		UpdateAt: entry.Entry.UpdateAt.Format("2006-01-02 15:04:05"),
	}
}

// WatchLock
// 观察锁
func (Watcher *Watcher) WatchLock(req *proto.KeyRequest, stream proto.CadbWatchHelper_WatchLockServer) error {
	value := metadata.ExtractIncoming(stream.Context()).Get(middleware.AuthKey)
	watcher, err := store.WatchLock(value, req.Key, func(WT store.WType, entry *store.Notification) {
		t := proto.WatchType(WT)

		stream.Send(&proto.KeyChange{
			Type: t,
			Key:  entry.Key,
			Data: Watcher.convert(entry),
		})
	})

	if err != nil {
		return err
	}

	// 阻塞
	select {
	case <-stream.Context().Done():
		{
			global.Log.Infof("通过终止会话[%s]结束会话 key: %s", value, req.Key)
			store.RemoveWatchLock(req.Key, value)
		}
	case <-watcher.Close:
		{
			global.Log.Infof("通过CloseWatch[%s]结束会话 key: %s", value, req.Key)
			store.RemoveWatchLock(req.Key, value)
		}
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
	watcher, err := store.Watch(value, req.Key, func(WT store.WType, entry *store.Notification) {
		t := proto.WatchType(WT)

		stream.Send(&proto.KeyChange{
			Type: t,
			Key:  entry.Key,
			Data: Watcher.convert(entry),
		})
	})

	if err != nil {
		return err
	}

	// 阻塞
	select {
	case <-stream.Context().Done():
		{
			global.Log.Infof("通过终止会话[%s]结束会话 key: %s", value, req.Key)
			store.RemoveWatch(req.Key, value)
		}
	case <-watcher.Close:
		{
			global.Log.Infof("通过CloseWatch[%s]结束会话 key: %s", value, req.Key)
			store.RemoveWatch(req.Key, value)
		}
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
