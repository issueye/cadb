package global

import (
	"go.uber.org/zap"
	"golang.corp.yxkj.com/orange/cadb/internal/store"
)

var (
	Store  *store.KVStore
	Logger *zap.Logger
	Log    *zap.SugaredLogger
)
