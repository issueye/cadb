package global

import (
	"github.com/r3labs/sse/v2"
	"golang.corp.yxkj.com/orange/cadb/internal/store"
)

var (
	Store *store.KVStore
	SSE   *sse.Server
)
