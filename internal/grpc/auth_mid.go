package grpc

import (
	"context"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"golang.corp.yxkj.com/orange/cadb/internal/global"
)

// CadbAuthFunc
// 鉴权中间件
func CadbAuthFunc(ctx context.Context) (context.Context, error) {
	token, err := grpc_auth.AuthFromMD(ctx, "bearer")
	if err != nil {
		return ctx, err
	}

	_, err = global.Store.Client.GetClientBySecretKey(token)
	if err != nil {
		return ctx, err
	}

	return ctx, nil
}
