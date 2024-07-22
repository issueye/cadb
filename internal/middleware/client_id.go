package middleware

import (
	"github.com/gin-gonic/gin"
	"golang.corp.yxkj.com/orange/cadb/internal/global"
)

// CheckClientId 检查客户端ID的中间件
func CheckClientId() func(c *gin.Context) {
	return func(c *gin.Context) {
		// 如果当前路由不是 /api/v1/newClient
		if c.Request.URL.Path != "/api/v1/client/new" {
			// 获取客户端ID
			clientId := c.GetHeader("secret-key")
			// 如果客户端ID不存在
			if clientId == "" {
				// 返回错误信息
				c.JSON(401, gin.H{
					"error": "secret-key is required",
				})
				// 终止请求
				c.Abort()
			} else {
				// 如果客户端ID存在
				ok := global.Store.ChekClientSecretKey(clientId)
				if !ok { // 如果客户端ID不存在
					c.JSON(401, gin.H{
						"error": "Invalid secret-key",
					})
					c.Abort()
				}
			}
		}
	}
}
