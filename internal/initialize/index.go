package initialize

import "golang.corp.yxkj.com/orange/cadb/internal/grpc"

// 初始化
func Init() {
	InitRuntime()

	InitLog()

	InitStore()

	go grpc.NewServer()

	r := Server()
	r.Run(":8080")
}
