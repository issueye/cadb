package store

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.etcd.io/bbolt"
	"golang.corp.yxkj.com/orange/cadb/internal/utils"
)

var DefaultBucket = []byte("kvstore")
var keysBucket = []byte("keys")

type KVEntry struct {
	Value string `json:"value"` // key 的值
	TTL   int64  `json:"ttl"`   // 过期时间
	Key   string `json:"key"`   // key
}

type KVStore struct {
	// bolt 数据库
	db *DB[KVEntry]
	// 过期索引
	expireIndex *ExpireIndex
	// TTL 类型 0 表示秒，1 表示毫秒
	ttlType int
	// 是否使用快照
	UseSnapshot bool
	// 快照路径
	SnapshotPath string
	// 存放被Watch的 key 以及对应的 channel
	watchedKeys map[string][]*Watcher
	// 被观察的key 锁
	watchedKeysLock *sync.Mutex
	// client
	Client *ClientStoreImpl
}

func NewKVStore(dbPath string) (*KVStore, error) {
	db, err := NewDB[KVEntry](dbPath)
	if err != nil {
		return nil, err
	}

	kv := &KVStore{
		db:              db,
		expireIndex:     NewExpireIndex(),
		watchedKeys:     make(map[string][]*Watcher),
		watchedKeysLock: &sync.Mutex{},
	}

	// 可能会 panic
	client, err := NewClientStoreImpl()
	if err != nil {
		return nil, err
	}

	kv.Client = client

	// 创建 keysBucket defaultBucket
	err = db.CreateBucket(keysBucket)
	if err != nil {
		return nil, fmt.Errorf("create bucket %s failed: %w", keysBucket, err)
	}

	err = db.CreateBucket(DefaultBucket)
	if err != nil {
		return nil, fmt.Errorf("create bucket %s failed: %w", DefaultBucket, err)
	}

	kv.startExpireLoop()

	return kv, nil
}

type CaKey struct {
	Key    []byte
	Bucket []byte
}

func (s *KVStore) ParseKey(key string) (caKey *CaKey) {
	keys := strings.Split(key, "-")
	if len(keys) == 1 {
		caKey = &CaKey{
			Key:    []byte(keys[0]),
			Bucket: DefaultBucket,
		}
	} else {
		caKey = &CaKey{Key: []byte(strings.Join(keys[1:], "-")), Bucket: []byte(keys[0])}
	}

	// 如果key 不是 DefaultBucket 需要判断 Bucket 是否存在，如果不存在则创建
	if bytes.Equal(DefaultBucket, caKey.Bucket) {
		s.db.CreateBucket(caKey.Bucket)
	}

	return
}

func (s *KVStore) CheckClientId(clientId string) bool {
	ok, err := s.Client.CheckClientIdExists(clientId)
	return ok && err == nil
}

func (s *KVStore) ChekClientSecretKey(secretKey string) bool {
	ok, err := s.Client.ChekClientSecretKeyExists(secretKey)
	return ok && err == nil
}

// Set 设置指定 key 的值
func (s *KVStore) Set(key, value string, ttl int) error {
	caKey := s.ParseKey(key)

	entry := &KVEntry{
		Key:   key,
		TTL:   int64(ttl),
		Value: value,
	}

	// 设置过期时间
	if ttl > 0 {
		if s.ttlType == 0 {
			entry.TTL = time.Now().Unix() + int64(ttl)
		} else {
			entry.TTL = time.Now().UnixNano()/1e6 + int64(ttl)
		}

		s.expireIndex.Add(key, entry.TTL)
	}

	// 将数据写入 BoltDB
	err := s.db.Put(caKey.Bucket, caKey.Key, entry)
	if err != nil {
		return err
	}

	// 存储一份 key 到 keysBucket 中
	err = s.db.db.Update(func(tx *bbolt.Tx) error { return tx.Bucket([]byte(keysBucket)).Put([]byte(key), []byte{}) })

	// 通知所有订阅了该 key 的订阅者
	s.Notify(&Notify{
		WT:    WT_Put,
		Key:   key,
		Entry: entry,
	})

	return nil
}

func (s *KVStore) Get(key string) (entry *KVEntry, err error) {
	caKey := s.ParseKey(key)

	entry, err = s.db.Get(caKey.Bucket, caKey.Key)
	if err != nil {
		return nil, err
	}

	// 检查数据是否过期
	if entry.TTL > 0 && time.Now().Unix() >= entry.TTL {
		return nil, fmt.Errorf("key %s has expired", key)
	}

	return entry, err
}

// Delete
// 删除指定 key 的数据，支持多级 key
func (s *KVStore) Delete(key string) error {
	caKey := s.ParseKey(key)

	entry, err := s.db.DeleteR(caKey.Bucket, caKey.Key)
	if err != nil {
		return err
	}

	// 如果数据被 Watch，则通知所有订阅者
	if s.HaveWatch(key) {
		s.Notify(&Notify{
			WT:    WT_Del,
			Key:   key,
			Entry: entry,
		})
	}

	// 移除 keysBucket 中的 key
	return s.db.Delete([]byte(keysBucket), []byte(key))
}

// RemoveTTL
// 移除指定 key 的 TTL
// 如果 key 不存在，则不返回错误
func (s *KVStore) RemoveTTL(key string) (err error) {
	caKey := s.ParseKey(key)

	s.db.UpdateFunc(caKey.Key, caKey.Bucket, func(bkt *bbolt.Bucket, v *KVEntry) error {
		// 如果数据已经过期
		if v.TTL > 0 && time.Now().Unix() >= v.TTL {
			return fmt.Errorf("key %s has expired", key)
		}

		// 如果数据被 Watch，则通知所有订阅者
		if s.HaveWatch(key) {
			s.Notify(&Notify{
				WT:    WT_ModifyExpire,
				Key:   key,
				Entry: v,
			})
		}

		// 移除过期缓存中的数据
		s.expireIndex.Remove(key)
		v.TTL = 0

		// 序列化
		data := utils.MustMarshal(v)
		return bkt.Put(caKey.Key, data)
	})

	return
}

// SetTTL
// 设置指定 key 的 TTL
func (s *KVStore) SetTTL(key string, ttl int64) (entry *KVEntry, err error) {
	caKey := s.ParseKey(key)

	err = s.db.UpdateFunc(caKey.Key, caKey.Bucket, func(bkt *bbolt.Bucket, v *KVEntry) error {
		// 设置 TTL
		entry.TTL = time.Now().Unix() + ttl
		s.expireIndex.Add(key, entry.TTL)

		// 序列化
		data := utils.MustMarshal(v)
		return bkt.Put(caKey.Key, data)
	})
	return
}

// Keys
// 获取所有 key
func (s *KVStore) Keys() (keys []string, err error) {
	err = s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(keysBucket)
		if b == nil {
			return nil
		}

		return b.ForEach(func(k, v []byte) error {
			keys = append(keys, string(k))
			return nil
		})
	})
	return
}
