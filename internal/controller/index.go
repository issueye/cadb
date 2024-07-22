package controller

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/r3labs/sse/v2"
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

// Watch
// 监听一个键的变化
func Watch(c *gin.Context) {
	key := c.DefaultQuery("stream", "")
	clientID := c.GetHeader("secret-key")

	keys := strings.Split(key, ":")
	if len(keys) != 2 {
		c.JSON(400, gin.H{"error": "invalid stream"})
		return
	}

	valueKey := keys[1]
	fmt.Println("clientID", clientID)

	watcher := global.Store.CheckWatch(clientID, valueKey)
	// fmt.Println("watcher", watcher.Id)
	if watcher == nil {
		watcher = global.Store.Watch(clientID, keys[1], func(WT store.WType, entry *store.KVEntry) {
			wr := WatchResponse{
				Key:   valueKey,
				Type:  WT,
				Value: entry.Value,
			}

			data := wr.ToData()
			fmt.Println("publish ->", valueKey, string(data))
			global.SSE.Publish(key, &sse.Event{Data: data})
		})
	}

	go func() {
		select {
		case <-c.Request.Context().Done():
			fmt.Println("Done -> client disconnect", clientID)
			global.Store.RemoveWatch(clientID, key)
		case <-c.Writer.CloseNotify():
			fmt.Println("CloseNotify -> client disconnect", clientID)
			global.Store.RemoveWatch(clientID, key)
		}
	}()

	global.SSE.ServeHTTP(c.Writer, c.Request)
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
