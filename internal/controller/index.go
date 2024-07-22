package controller

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/gin-gonic/gin"
	"golang.corp.yxkj.com/orange/cadb/internal/global"
	"golang.corp.yxkj.com/orange/cadb/internal/store"
)

// Index is the handler for the root path
// It returns a JSON response with a message
func Index(c *gin.Context) { c.JSON(200, gin.H{"message": "Hello World"}) }

// Set
// 设置一个键值对
func Set(c *gin.Context) {
	// 需要在 header 中添加 secret-key，用于标识客户端
	key := c.Param("key")
	ttl := c.DefaultQuery("ttl", "0")
	ttlInt, _ := strconv.Atoi(ttl)

	requestData := make(map[string]string)
	err := c.Bind(&requestData)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
	}

	fmt.Println("requestData", requestData)

	value := requestData["value"]

	err = global.Store.Set(key, value, ttlInt)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	c.JSON(200, gin.H{"message": "OK"})
}

// Get
// 获取一个键的值
func Get(c *gin.Context) {
	key := c.Param("key")
	value, err := global.Store.Get(key)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	c.JSON(200, gin.H{"value": value})
}

// Delete
// 删除一个键
func Delete(c *gin.Context) {
	key := c.Param("key")
	err := global.Store.Delete(key)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	c.JSON(200, gin.H{"message": "OK"})
}

// Keys
// 获取所有键
func Keys(c *gin.Context) {
	keys, err := global.Store.Keys()
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	c.JSON(200, gin.H{"keys": keys})
}

// SetTTL
// 设置一个键的 TTL
func SetTTL(c *gin.Context) {
	key := c.Param("key")
	ttl := c.Param("ttl")
	ttlInt, err := strconv.ParseInt(ttl, 10, 64)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	value, err := global.Store.SetTTL(key, ttlInt)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	c.JSON(200, gin.H{"value": value})
}

// RemoveTTL
// 移除一个键的 TTL
func RemoveTTL(c *gin.Context) {
	key := c.Param("key")
	err := global.Store.RemoveTTL(key)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	c.JSON(200, gin.H{"message": "OK"})
}

type WatchResponse struct {
	Key   string      `json:"key"`
	Type  store.WType `json:"type"`
	Value string      `json:"value"`
}

func (wr *WatchResponse) ToData() []byte {
	data, err := json.Marshal(wr)
	if err != nil {
		return []byte{}
	}
	return data
}

// NewClient
// 创建一个新的客户端
func NewClient(c *gin.Context) {
	client, err := global.Store.Client.CreateClient()
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
	}

	c.JSON(200, gin.H{"secret-key": client.SecretKey})
}
