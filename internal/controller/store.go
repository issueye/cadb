package controller

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/metadata"
	"golang.corp.yxkj.com/orange/cadb/internal/global"
	"golang.corp.yxkj.com/orange/cadb/internal/grpc/proto/proto"
	"golang.corp.yxkj.com/orange/cadb/internal/store"
)

type Server struct{}

// 获取数据
func (srv *Server) Get(ctx context.Context, req *proto.KeyRequest) (*proto.GetResponse, error) {
	data, err := global.Store.Get(req.Key)
	if err != nil {
		return nil, err
	}

	return &proto.GetResponse{Data: data.Value}, nil
}

// 设置数据
func (srv *Server) Set(ctx context.Context, req *proto.SetRequest) (*proto.Empty, error) {
	err := global.Store.Set(req.Key, req.Data, req.Expire)
	return &proto.Empty{}, err
}

// 删除数据
func (srv *Server) Delete(ctx context.Context, req *proto.KeyRequest) (*proto.Empty, error) {
	err := global.Store.Delete(req.Key)
	if err != nil {
		return nil, err
	}

	return &proto.Empty{}, nil
}

// 获取 keys
func (srv *Server) Keys(ctx context.Context, empty *proto.Empty) (*proto.KeysResponse, error) {
	keys, err := global.Store.Keys()
	if err != nil {
		return nil, err
	}

	return &proto.KeysResponse{Data: keys}, nil
}

// 设置key的过期时间
func (srv *Server) AddExpire(ctx context.Context, req *proto.AddExpireRequest) (*proto.Empty, error) {
	_, err := global.Store.SetTTL(req.Key, req.Expire)
	if err != nil {
		return nil, err
	}

	return &proto.Empty{}, nil
}

// 移除 key的过期时间
func (srv *Server) MoveExpire(ctx context.Context, req *proto.MoveExpireRequest) (*proto.Empty, error) {
	err := global.Store.RemoveTTL(req.Key)
	if err != nil {
		return nil, err
	}
	return &proto.Empty{}, nil
}

// 监听 key的变化
func (srv *Server) Watch(req *proto.KeyRequest, stream proto.CadbStoreHelper_WatchServer) error {
	value := metadata.ExtractIncoming(stream.Context()).Get("secret-key")

	watcher := global.Store.Watch(value, req.Key, func(WT store.WType, entry *store.KVEntry) {
		t := proto.WatchType(WT)
		stream.Send(&proto.KeyChange{
			Type: t,
			Key:  entry.Key,
			Data: entry.Value,
		})
	})

	// 阻塞
	select {
	case <-stream.Context().Done():
		{
			global.Log.Infof("通过终止会话[%s]结束会话 key: %s", value, req.Key)
			global.Store.RemoveWatch(req.Key, value)
		}
	case <-watcher.Close:
		global.Log.Infof("通过CloseWatch[%s]结束会话 key: %s", value, req.Key)
	}
	return nil
}

// 关闭监听
func (srv *Server) CloseWatch(ctx context.Context, req *proto.KeyRequest) (*proto.Empty, error) {
	value := metadata.ExtractIncoming(ctx).Get("secret-key")
	global.Store.RemoveWatch(req.Key, value)
	return &proto.Empty{}, nil
}
