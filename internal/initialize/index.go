package initialize

// 初始化
func Init() {
	InitStore()
	InitSSE()

	r := Server()
	r.Run(":8080")
}
