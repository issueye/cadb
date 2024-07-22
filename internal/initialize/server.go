package initialize

import (
	"github.com/gin-gonic/gin"
	"golang.corp.yxkj.com/orange/cadb/internal/controller"
	"golang.corp.yxkj.com/orange/cadb/internal/middleware"
)

func Server() *gin.Engine {
	r := gin.Default()
	r.Use(gin.Logger())
	r.Use(gin.Recovery())
	r.Use(middleware.CORS())
	r.Use(middleware.CheckClientId())

	v1 := r.Group("/api/v1")
	{
		v1.GET("/ping", func(c *gin.Context) { c.JSON(200, gin.H{"message": "pong"}) })
		v1.GET("/version", func(c *gin.Context) { c.JSON(200, gin.H{"version": "1.0.0"}) })

		store := v1.Group("/store")
		{
			store.POST("/set/:key", controller.Set)
			store.GET("/get/:key", controller.Get)
			store.DELETE("/delete/:key", controller.Delete)
			store.GET("/keys", controller.Keys)
			store.PUT("/expire/:key/:ttl", controller.SetTTL)
			store.DELETE("/expire/:key", controller.RemoveTTL)
			store.GET("watch", controller.Watch)
		}

		client := v1.Group("/client")
		{
			client.GET("new", controller.NewClient)
		}
	}

	return r
}
