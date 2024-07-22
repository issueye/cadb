package global

import (
	"github.com/r3labs/sse/v2"
	"go.uber.org/zap"
	"golang.corp.yxkj.com/orange/cadb/internal/store"
)

var (
	Store  *store.KVStore
	SSE    *sse.Server
	Logger *zap.Logger
	Log    *zap.SugaredLogger
)
