package store

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"go.etcd.io/bbolt"
	"golang.corp.yxkj.com/orange/cadb/internal/utils"
)

var DefaultBucket = "kvstore"
var keysBucket = "keys"

type KVEntry struct {
	Value string
	// 过期时间
	TTL int64
	// TTL 类型 0 表示秒，1 表示毫秒
	ttlType int
	// key 的各个层级
	Keys []string
}

type KVStore struct {
	// bolt 数据库
	db *bbolt.DB
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
}

func NewKVStore(dbPath string) (*KVStore, error) {
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, err
	}
	kv := &KVStore{
		db:              db,
		expireIndex:     NewExpireIndex(),
		watchedKeys:     make(map[string][]*Watcher),
		watchedKeysLock: &sync.Mutex{},
	}

	// 创建 keysBucket defaultBucket
	err = kv.db.Update(func(tx *bbolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte(keysBucket))
		tx.CreateBucketIfNotExists([]byte(DefaultBucket))
		return nil
	})

	kv.startExpireLoop()

	return kv, nil
}

type CaKey struct {
	Key    string
	Bucket string
}

func (s *KVStore) ParseKey(key string) (caKey *CaKey) {
	keys := strings.Split(key, "/")
	if len(keys) == 1 {
		caKey = &CaKey{
			Key:    keys[0],
			Bucket: DefaultBucket,
		}
	} else {
		caKey = &CaKey{Key: strings.Join(keys[1:], "/"), Bucket: keys[0]}
	}

	// 如果key 不是 DefaultBucket 需要判断 Bucket 是否存在，如果不存在则创建
	if caKey.Bucket != DefaultBucket {

		s.db.Update(func(tx *bbolt.Tx) error {
			tx.CreateBucketIfNotExists([]byte(caKey.Bucket))
			return nil
		})
	}

	return
}

// Set 设置指定 key 的值
func (s *KVStore) Set(key, value string, ttl int) error {
	caKey := s.ParseKey(key)

	entry := &KVEntry{
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
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(caKey.Bucket))
		return b.Put([]byte(caKey.Key), utils.MustMarshal(entry))
	})
	if err != nil {
		return err
	}

	// 存储一份 key 到 keysBucket 中
	err = s.db.Update(func(tx *bbolt.Tx) error { return tx.Bucket([]byte(keysBucket)).Put([]byte(key), []byte{}) })

	// 通知所有订阅了该 key 的订阅者
	s.Notify(&Notify{
		WT:    WT_Put,
		Key:   key,
		Entry: entry,
	})

	return nil
}

func (s *KVStore) Get(key string) (*KVEntry, error) {
	caKey := s.ParseKey(key)

	var entry *KVEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(caKey.Bucket))

		// 获取数据
		data := b.Get([]byte(caKey.Key))
		if data == nil {
			return nil
		}

		// 反序列化
		if err := utils.UnMarshal(data, &entry); err != nil {
			return err
		}

		// 检查是否过期
		if entry.TTL > 0 && time.Now().Unix() >= entry.TTL {
			return fmt.Errorf("key %s has expired", key)
		}
		return nil
	})
	return entry, err
}

// Delete
// 删除指定 key 的数据，支持多级 key
func (s *KVStore) Delete(key string) error {
	caKey := s.ParseKey(key)

	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(caKey.Bucket))
		// 获取数据
		data := b.Get([]byte(caKey.Key))
		if data == nil {
			return nil
		}

		// 反序列化
		var entry *KVEntry
		if err := utils.UnMarshal(data, &entry); err != nil {
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
		err := s.db.Update(func(tx *bbolt.Tx) error { return tx.Bucket([]byte(keysBucket)).Delete([]byte(key)) })
		if err != nil {
			return err
		}

		return b.Delete([]byte(caKey.Key))
	})
}

// RemoveTTL
// 移除指定 key 的 TTL
// 如果 key 不存在，则不返回错误
func (s *KVStore) RemoveTTL(key string) (err error) {
	caKey := s.ParseKey(key)

	err = s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(caKey.Bucket))
		// 获取数据
		data := b.Get([]byte(caKey.Key))
		if data == nil {
			return nil
		}

		// 反序列化
		var entry *KVEntry
		if err := utils.UnMarshal(data, &entry); err != nil {
			return err
		}

		// 如果数据已经过期
		if entry.TTL > 0 && time.Now().Unix() >= entry.TTL {
			return fmt.Errorf("key %s has expired", key)
		}

		// 如果数据被 Watch，则通知所有订阅者
		if s.HaveWatch(key) {
			s.Notify(&Notify{
				WT:    WT_ModifyExpire,
				Key:   key,
				Entry: entry,
			})
		}

		// 移除过期缓存中的数据
		s.expireIndex.Remove(key)
		entry.TTL = 0

		// 序列化
		data = utils.MustMarshal(entry)
		return b.Put([]byte(caKey.Key), data)
	})

	return
}

// SetTTL
// 设置指定 key 的 TTL
func (s *KVStore) SetTTL(key string, ttl int64) (entry *KVEntry, err error) {
	caKey := s.ParseKey(key)
	err = s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(caKey.Bucket))
		// 获取数据
		data := b.Get([]byte(caKey.Key))
		if data == nil {
			return nil
		}

		// 反序列化
		if err := utils.UnMarshal(data, &entry); err != nil {
			return err
		}
		// 设置 TTL
		entry.TTL = time.Now().Unix() + ttl
		s.expireIndex.Add(key, entry.TTL)
		return nil
	})

	return
}

// Keys
// 获取所有 key
func (s *KVStore) Keys() (keys []string, err error) {
	err = s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(keysBucket))
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
