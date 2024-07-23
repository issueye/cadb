package controller

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/metadata"
	"golang.corp.yxkj.com/orange/cadb/internal/global"
	"golang.corp.yxkj.com/orange/cadb/internal/grpc/proto/proto"
	"golang.corp.yxkj.com/orange/cadb/internal/middleware"
)

type CadbLock struct{}

// 锁
func (lock *CadbLock) Lock(ctx context.Context, req *proto.LockEntryRequest) (*proto.LockEntryResponse, error) {
	clientId := metadata.ExtractIncoming(ctx).Get(middleware.AuthKey)
	entry, err := global.Store.Lock(clientId, req.Key, req.Ttl)
	if err != nil {
		return nil, err
	}

	return &proto.LockEntryResponse{
		Key:     entry.Key,
		Ttl:     entry.TTL,
		LeaseID: entry.LeaseID,
		Lock:    entry.Lock,
	}, nil
}

func (lock *CadbLock) Unlock(ctx context.Context, req *proto.UnLockEntryRequest) (*proto.Empty, error) {
	clientId := metadata.ExtractIncoming(ctx).Get(middleware.AuthKey)
	return nil, global.Store.Unlock(clientId, req.LeaseID, req.Key)
}

func (lock *CadbLock) RenewLock(ctx context.Context, req *proto.RenewLockEntryRequest) (*proto.LockEntryResponse, error) {
	clientId := metadata.ExtractIncoming(ctx).Get(middleware.AuthKey)
	entry, err := global.Store.RenewLock(clientId, req.Key, req.LeaseID, req.Ttl)
	if err != nil {
		return nil, err
	}
	return &proto.LockEntryResponse{
		Key:     entry.Key,
		Ttl:     entry.TTL,
		LeaseID: entry.LeaseID,
		Lock:    entry.Lock,
	}, nil
}

func (lock *CadbLock) TryLock(ctx context.Context, req *proto.LockEntryRequest) (*proto.LockEntryResponse, error) {
	clientId := metadata.ExtractIncoming(ctx).Get(middleware.AuthKey)
	entry, err := global.Store.TryLock(clientId, req.Key, req.Ttl)
	if err != nil {
		return nil, err
	}
	return &proto.LockEntryResponse{
		Key:     entry.Key,
		Ttl:     entry.TTL,
		LeaseID: entry.LeaseID,
		Lock:    entry.Lock,
	}, nil
}
