package middleware

import (
	"context"
	"fmt"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/metadata"
	"golang.corp.yxkj.com/orange/cadb/internal/global"
	"google.golang.org/grpc"
)

const (
	AuthKey         = "secret-key"
	NewClientMethod = "/proto.CadbClientHelper/NewClient"
)

func AuthMiddleware(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// 获取当前请求的 gRPC 方法名
	methodName := info.FullMethod
	// 根据方法名判断是否需要鉴权
	if isPublicMethod(methodName) {
		// 不需要鉴权的方法直接放行
		return handler(ctx, req)
	}

	// 需要鉴权的方法进行鉴权逻辑
	newCtx, err := checkClientSecretKey(ctx)
	if err != nil {
		return nil, err
	}

	// 鉴权通过后，调用实际的 handler 处理请求
	return handler(newCtx, req)
}

func checkClientSecretKey(ctx context.Context) (newCtx context.Context, err error) {
	// 获取加密key
	secretKey := metadata.ExtractIncoming(ctx).Get(AuthKey)
	if secretKey == "" {
		// 如果没有提供加密key，则返回鉴权失败
		return nil, fmt.Errorf("unauthorized access, secret key is required")
	}

	isHave := global.Store.CheckClientSecretKey(secretKey)
	if !isHave {
		return nil, fmt.Errorf("unauthorized access, secret key is invalid")
	}

	newCtx = ctx

	return newCtx, nil
}

func isPublicMethod(methodName string) bool {
	// 在这里定义不需要鉴权的方法列表
	publicMethods := []string{
		NewClientMethod,
	}

	for _, m := range publicMethods {
		if m == methodName {
			return true
		}
	}
	return false
}

func AuthStreamMiddleware(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// 获取当前请求的 gRPC 方法名
	methodName := info.FullMethod

	// 根据方法名判断是否需要鉴权
	if isPublicMethod(methodName) {
		// 不需要鉴权的方法直接放行
		return handler(srv, stream)
	}

	// 需要鉴权的方法进行鉴权逻辑
	// 需要鉴权的方法进行鉴权逻辑
	_, err := checkClientSecretKey(stream.Context())
	if err != nil {
		return err
	}

	// 将返回的 context 传递给 stream
	stream = grpc_middleware.WrapServerStream(stream)

	// 鉴权通过后，调用实际的 handler 处理请求
	return handler(srv, stream)
}
