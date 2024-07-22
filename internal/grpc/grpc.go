package grpc

import (
	"context"
	"log"
	"net"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"golang.corp.yxkj.com/orange/cadb/internal/global"
	"golang.corp.yxkj.com/orange/cadb/internal/grpc/proto/proto"
	"golang.corp.yxkj.com/orange/cadb/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	port = ":50051"
)

type Server struct{}

// NewServer creates a new gRPC server.
func NewServer() {
	opts := []logging.Option{
		logging.WithLogOnEvents(logging.StartCall, logging.FinishCall),
		// Add any other option (check functions starting with logging.With).
	}

	grpcSrv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			auth.UnaryServerInterceptor(CadbAuthFunc),
			grpc_recovery.UnaryServerInterceptor(),
			logging.UnaryServerInterceptor(InterceptorLogger(global.Logger), opts...),
		),

		grpc.ChainStreamInterceptor(
			auth.StreamServerInterceptor(CadbAuthFunc),
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
func (srv *Server) NewClient(ctx context.Context, empty *proto.Empty) (*proto.ClientResponse, error) {
	client, err := global.Store.Client.CreateClient()
	if err != nil {
		return nil, err
	}

	return &proto.ClientResponse{SecretKey: client.SecretKey}, nil
}

// 关闭客户端
func (srv *Server) CloseClient(ctx context.Context, empty *proto.Empty) (*proto.Empty, error) {
	token, err := auth.AuthFromMD(ctx, "")
	if err != nil {
		return nil, err
	}

	return &proto.Empty{}, global.Store.Client.DeleteClient(token)
}

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
	err := global.Store.Set(req.Key, req.Data, int(req.Expire))
	if err != nil {
		return nil, err
	}

	return &proto.Empty{}, nil
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
func (srv *Server) Expire(ctx context.Context, req *proto.ExpireRequest) (*proto.Empty, error) {
	_, err := global.Store.SetTTL(req.Key, req.Expire)
	if err != nil {
		return nil, err
	}

	return &proto.Empty{}, nil
}

// 移除 key的过期时间
func (srv *Server) Persist(ctx context.Context, req *proto.PersistRequest) (*proto.Empty, error) {
	err := global.Store.RemoveTTL(req.Key)
	if err != nil {
		return nil, err
	}
	return &proto.Empty{}, nil
}

// 监听 key的变化
func (srv *Server) Watch(req *proto.KeyRequest, stream proto.CadbStoreHelper_WatchServer) error {
	token, err := auth.AuthFromMD(stream.Context(), "bearer")
	if err != nil {
		return err
	}

	global.Store.Watch(token, req.Key, func(WT store.WType, entry *store.KVEntry) {
		t := proto.WatchType(WT)
		stream.Send(&proto.KeyChange{
			Type: t,
			Key:  entry.Key,
			Data: entry.Value,
		})
	})

	<-stream.Context().Done()
	global.Store.RemoveWatch(req.Key, token)
	return nil
}

// 关闭监听
func (srv *Server) CloseWatch(ctx context.Context, req *proto.KeyRequest) (*proto.Empty, error) {
	id := ctx.Value("id").(string)
	global.Store.RemoveWatch(req.Key, id)
	return &proto.Empty{}, nil
}
