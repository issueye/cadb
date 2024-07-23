package client

import (
	"context"

	"golang.corp.yxkj.com/orange/cadb/internal/grpc/proto/proto"
	"google.golang.org/grpc"
)

type CadbClient struct {
	clientHelper proto.CadbClientHelperClient
	storeHelper  proto.CadbStoreHelperClient
	secretKey    string
}

func NewCadbClient(addr string) (*CadbClient, error) {
	// 连接 gRPC 服务器
	conn, err := grpc.Dial(
		addr,
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(appendSecretKeyToMetadata),
		grpc.WithStreamInterceptor(appendStreamSecretKeyToMetadata),
	)
	if err != nil {
		return nil, err
	}

	// 创建 CadbClientHelper 客户端
	clientHelper := proto.NewCadbClientHelperClient(conn)

	// 创建新的客户端
	resp, err := clientHelper.NewClient(context.Background(), &proto.Empty{})
	if err != nil {
		return nil, err
	}

	token = resp.SecretKey

	// 创建 CadbStoreHelper 客户端
	storeHelper := proto.NewCadbStoreHelperClient(conn)

	return &CadbClient{
		clientHelper: clientHelper,
		storeHelper:  storeHelper,
		secretKey:    resp.SecretKey,
	}, nil
}

func (c *CadbClient) Close() error {
	_, err := c.clientHelper.CloseClient(context.Background(), &proto.Empty{})
	return err
}

func (c *CadbClient) Get(key string) (string, error) {
	resp, err := c.storeHelper.Get(context.Background(), &proto.KeyRequest{
		Key: key,
	})
	if err != nil {
		return "", err
	}
	return resp.Data, nil
}

func (c *CadbClient) Set(key, data string, expire int64) error {
	_, err := c.storeHelper.Set(context.Background(), &proto.SetRequest{
		Key:    key,
		Data:   data,
		Expire: expire,
	})
	return err
}

func (c *CadbClient) Delete(key string) error {
	_, err := c.storeHelper.Delete(context.Background(), &proto.KeyRequest{
		Key: key,
	})
	return err
}

func (c *CadbClient) Keys() ([]string, error) {
	resp, err := c.storeHelper.Keys(context.Background(), &proto.Empty{})
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (c *CadbClient) AddExpire(key string, expire int64) error {
	_, err := c.storeHelper.AddExpire(context.Background(), &proto.AddExpireRequest{
		Key:    key,
		Expire: expire,
	})
	return err
}

func (c *CadbClient) MoveExpire(key string) error {
	_, err := c.storeHelper.MoveExpire(context.Background(), &proto.MoveExpireRequest{
		Key: key,
	})
	return err
}

func (c *CadbClient) Watch(key string) (proto.CadbStoreHelper_WatchClient, error) {
	return c.storeHelper.Watch(context.Background(), &proto.KeyRequest{
		Key: key,
	})
}

func (c *CadbClient) CloseWatch(key string) error {
	_, err := c.storeHelper.CloseWatch(context.Background(), &proto.KeyRequest{
		Key: key,
	})
	return err
}
