package grpc

import (
	"context"
	"log"
	"net"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"golang.corp.yxkj.com/orange/cadb/internal/global"
	"golang.corp.yxkj.com/orange/cadb/internal/grpc/proto/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	port = ":50051"
)

type Server struct{}

// NewServer creates a new gRPC server.
func NewServer() {
	grpcSrv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			grpc_auth.UnaryServerInterceptor(CadbAuthFunc),
			grpc_recovery.UnaryServerInterceptor(),
		),

		grpc.ChainStreamInterceptor(
			grpc_auth.StreamServerInterceptor(CadbAuthFunc),
			grpc_recovery.StreamServerInterceptor(),
		),
	)

	listen, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	proto.RegisterCadbStoreHelperServer(grpcSrv, &Server{})
	reflection.Register(grpcSrv)
	err = grpcSrv.Serve(listen)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// 新建客户端
func (srv *Server) NewClient(context.Context, *proto.Empty) (*proto.ClientResponse, error) {
	client, err := global.Store.Client.CreateClient()
	if err != nil {
		return nil, err
	}

	return &proto.ClientResponse{SecretKey: client.SecretKey}, nil
}

// 关闭客户端
func (srv *Server) CloseClient(context.Context, *proto.Empty) (*proto.Empty, error) {
	// global.Store.Client.DeleteClient()
	return &proto.Empty{}, nil
}

// 获取数据
func (srv *Server) Get(context.Context, *proto.KeyRequest) (*proto.GetResponse, error)

// 设置数据
func (srv *Server) Set(context.Context, *proto.SetRequest) (*proto.Empty, error)

// 删除数据
func (srv *Server) Delete(context.Context, *proto.KeyRequest) (*proto.Empty, error)

// 获取 keys
func (srv *Server) Keys(context.Context, *proto.Empty) (*proto.GetResponse, error)

// 设置key的过期时间
func (srv *Server) Expire(context.Context, *proto.KeyRequest) (*proto.Empty, error)

// 移除 key的过期时间
func (srv *Server) Persist(context.Context, *proto.KeyRequest) (*proto.Empty, error)

// 监听 key的变化
func (srv *Server) Watch(context.Context, *proto.KeyRequest) (*proto.GetResponse, error)
