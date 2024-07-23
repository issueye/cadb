package initialize

import (
	_ "embed"
	"fmt"
	"log"
	"net"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"golang.corp.yxkj.com/orange/cadb/internal/controller"
	"golang.corp.yxkj.com/orange/cadb/internal/global"
	"golang.corp.yxkj.com/orange/cadb/internal/grpc/proto/proto"
	"golang.corp.yxkj.com/orange/cadb/internal/middleware"
	"golang.corp.yxkj.com/orange/cadb/pkg/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	port = ":50051"
)

func InitGrpcServer() {
	NewServer()
}

// NewServer creates a new gRPC server.
func NewServer() {
	opts := []logging.Option{
		logging.WithLogOnEvents(logging.StartCall, logging.FinishCall),
		// Add any other option (check functions starting with logging.With).
	}

	grpcSrv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			middleware.AuthMiddleware,
			grpc_recovery.UnaryServerInterceptor(),
			logging.UnaryServerInterceptor(middleware.InterceptorLogger(global.Logger), opts...),
		),

		grpc.ChainStreamInterceptor(
			middleware.AuthStreamMiddleware,
			grpc_recovery.StreamServerInterceptor(),
		),
	)

	listen, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	proto.RegisterCommonHelperServer(grpcSrv, &controller.CommonHelper{})
	proto.RegisterCadbStoreHelperServer(grpcSrv, &controller.Server{})
	proto.RegisterCadbClientHelperServer(grpcSrv, &controller.Server{})
	proto.RegisterCadbLockHelperServer(grpcSrv, &controller.CadbLock{})
	reflection.Register(grpcSrv)

	ShowBaner()

	err = grpcSrv.Serve(listen)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

//go:embed banner.txt
var banner []byte

func ShowBaner() {
	fmt.Println(utils.Bytes2String(banner))
}
