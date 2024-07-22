package store

import (
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/google/uuid"
	"golang.corp.yxkj.com/orange/cadb/internal/utils"
)

var ClientKey = []byte("client")

// Client
// 客户端
type Client struct {
	ID        string    `json:"id"`         // id 编码
	SecretKey string    `json:"secret_key"` // 秘钥
	CreatedAt time.Time `json:"created_at"` // 创建时间
}

// ClientStore 客户端存储接口
type ClientStore interface {
	CreateClient() (*Client, error)
	GetClientByID(id string) (*Client, error)
	GetClientBySecretKey(secretKey string) (*Client, error)
	UpdateClient(c *Client) error
	DeleteClient(id string) error
	CheckClientIdExists(id string) (bool, error)
	ChekClientSecretKeyExists(secretKey string) (bool, error)
}

// ClientStoreImpl 客户端存储实现
type ClientStoreImpl struct {
	db *DB[Client]
}

// NewClientStoreImpl 创建客户端存储实现
func NewClientStoreImpl() (*ClientStoreImpl, error) {
	path := filepath.Join("runtime", "db", "client.db")

	db, err := NewDB[Client](path)
	if err != nil {
		return nil, err
	}

	db.CreateBucket(ClientKey)

	return &ClientStoreImpl{
		db: db,
	}, nil
}

// CreateClient 创建客户端
func (s *ClientStoreImpl) CreateClient() (*Client, error) {
	client := &Client{}
	client.ID = uuid.New().String()
	client.CreatedAt = time.Now()

	// secretkey = sha256(client.id + client.created_at)
	micro := client.CreatedAt.UnixMicro()
	client.SecretKey = utils.SHA1(client.ID + strconv.FormatInt(micro, 10))

	// 存储客户端
	err := s.db.Put(ClientKey, []byte(client.ID), client)
	if err != nil {
		return nil, err
	}
	// 返回客户端
	return client, nil
}

// CheckClientIdExists(id string) (bool, error)
func (s *ClientStoreImpl) CheckClientIdExists(id string) (bool, error) {
	data, err := s.db.Get(ClientKey, []byte(id))
	return data != nil, err
}

// ChekClientScecretKeyExists(secretKey string) (bool, error)
func (s *ClientStoreImpl) ChekClientSecretKeyExists(secretKey string) (bool, error) {
	data, err := s.GetClientBySecretKey(secretKey)
	fmt.Println("data", data)
	return data != nil, err
}

// GetClientByID 根据id获取客户端
func (s *ClientStoreImpl) GetClientByID(id string) (client *Client, err error) {
	client, err = s.db.Get(ClientKey, []byte(id))
	return
}

// GetClientBySecretKey 根据秘钥获取客户端
func (s *ClientStoreImpl) GetClientBySecretKey(secretKey string) (client *Client, err error) {
	err = s.db.ForEach(ClientKey, func(k []byte, v *Client) (bool, error) {
		if utils.SHA1(string(k)+strconv.FormatInt(v.CreatedAt.UnixMicro(), 10)) == secretKey {
			client = v
			return true, nil
		}

		return false, nil
	})

	return
}

// UpdateClient 更新客户端
func (s *ClientStoreImpl) UpdateClient(c *Client) error {
	return s.db.Put(ClientKey, []byte(c.ID), c)
}

// DeleteClient 删除客户端
func (s *ClientStoreImpl) DeleteClient(id string) error {
	return s.db.Delete(ClientKey, []byte(id))
}
