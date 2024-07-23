package client

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/metadata"
	"google.golang.org/grpc"
)

var token string = ""

// 中间件函数,在每个 RPC 调用中将 secretKey 添加到 metadata
func appendSecretKeyToMetadata(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// fmt.Println("token", token)
	if token != "" {
		// 如果 token 不为空，则将其添加到 metadata 中
		md := metadata.ExtractOutgoing(ctx).Add("secret-key", token)
		newCtx := md.ToOutgoing(ctx)

		return invoker(newCtx, method, req, reply, cc, opts...)
	} else {
		// 如果 token 为空，则直接调用实际的 RPC 方法
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func appendStreamSecretKeyToMetadata(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if token != "" {
		md := metadata.ExtractOutgoing(ctx).Add("secret-key", token)
		newCtx := md.ToOutgoing(ctx)
		return streamer(newCtx, desc, cc, method, opts...)
	} else {
		return streamer(ctx, desc, cc, method, opts...)
	}
}
