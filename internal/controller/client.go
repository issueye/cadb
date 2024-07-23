package controller

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/metadata"
	"golang.corp.yxkj.com/orange/cadb/internal/global"
	"golang.corp.yxkj.com/orange/cadb/internal/grpc/proto/proto"
	"golang.corp.yxkj.com/orange/cadb/internal/middleware"
)

// 新建客户端
func (srv *Server) NewClient(ctx context.Context, empty *proto.Empty) (*proto.ClientResponse, error) {
	client, err := global.Store.Client.CreateClient()
	if err != nil {
		return nil, err
	}

	return &proto.ClientResponse{SecretKey: client.SecretKey}, nil
}

// 关闭客户端
func (srv *Server) CloseClient(ctx context.Context, empty *proto.Empty) (*proto.Empty, error) {
	// 获取 secretKey
	value := metadata.ExtractIncoming(ctx).Get(middleware.AuthKey)
	return &proto.Empty{}, global.Store.Client.DeleteClient(value)
}
