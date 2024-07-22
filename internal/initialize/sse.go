package initialize

import (
	"fmt"

	"github.com/r3labs/sse/v2"
	"golang.corp.yxkj.com/orange/cadb/internal/global"
)

func InitSSE() {
	global.SSE = sse.NewWithCallback(OnSubscribe, OnUnsubscribe)
	global.SSE.AutoStream = true
}

// OnSubscribe
// 连接
func OnSubscribe(streamID string, sub *sse.Subscriber) {
	fmt.Println("sse subscribe: ", streamID)
}

// OnUnsubscribe
// 断开连接
func OnUnsubscribe(streamID string, sub *sse.Subscriber) {
	fmt.Println("sse unsubscribe: ", streamID)
}
