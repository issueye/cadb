package initialize

// 初始化
func Init() {
	// 初始化运行时目录
	InitRuntime()
	// 初始化日志配置
	InitLog()
	// 初始化数据库
	InitStore()
	// 初始化grpc服务
	InitGrpcServer()
}
