package store

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.etcd.io/bbolt"
	"golang.corp.yxkj.com/orange/cadb/internal/utils"
	pkgUtils "golang.corp.yxkj.com/orange/cadb/pkg/utils"
)

var DefaultBucket = []byte("kvstore")
var keysBucket = []byte("keys")

type KVEntry struct {
	Value string `json:"value"` // key 的值
	TTL   int64  `json:"ttl"`   // 过期时间
	Key   string `json:"key"`   // key
}

// 用于锁定 key 的结构体
type LockKVEntry struct {
	Value   string `json:"value"`    // key 的值  存储客户端的ID
	TTL     int64  `json:"ttl"`      // 过期时间
	Key     string `json:"key"`      // key
	Lock    bool   `json:"lock"`     // 是否被锁定
	LeaseID int64  `json:"lease_id"` // 租约ID
}

type KVStore struct {
	// bolt 数据库
	db *DB[KVEntry]
	// bolt 数据库
	lockdb *DB[LockKVEntry]
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

	lockdb, err := NewDB[LockKVEntry](fmt.Sprintf("%s.lock", dbPath))
	if err != nil {
		return nil, err
	}

	kv := &KVStore{
		db:              db,
		lockdb:          lockdb,
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

	err = lockdb.CreateBucket(DefaultBucket)
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

func (s *KVStore) ParseKey(key string, isLock bool) (caKey *CaKey) {
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
	if !bytes.Equal(DefaultBucket, caKey.Bucket) {
		if isLock {
			s.lockdb.CreateBucket(caKey.Bucket)
		} else {
			s.db.CreateBucket(caKey.Bucket)
		}
	}

	return
}

func (s *KVStore) CheckClientId(clientId string) bool {
	ok, err := s.Client.CheckClientIdExists(clientId)
	return ok && err == nil
}

func (s *KVStore) CheckClientSecretKey(secretKey string) bool {
	ok, err := s.Client.ChekClientSecretKeyExists(secretKey)
	return ok && err == nil
}

// Set 设置指定 key 的值
func (s *KVStore) Set(key, value string, ttl int) error {
	caKey := s.ParseKey(key, false)

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
		WT:    WT_PUT,
		Key:   key,
		Entry: entry,
	})

	return nil
}

func (s *KVStore) Get(key string) (entry *KVEntry, err error) {
	caKey := s.ParseKey(key, false)

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
	caKey := s.ParseKey(key, false)

	entry, err := s.db.DeleteR(caKey.Bucket, caKey.Key)
	if err != nil {
		return err
	}

	// 如果数据被 Watch，则通知所有订阅者
	if s.HaveWatch(key) {
		s.Notify(&Notify{
			WT:    WT_DELETE,
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
	caKey := s.ParseKey(key, false)

	s.db.UpdateFunc(caKey.Key, caKey.Bucket, func(bkt *bbolt.Bucket, v *KVEntry) error {
		// 如果数据已经过期
		if v.TTL > 0 && time.Now().Unix() >= v.TTL {
			return fmt.Errorf("key %s has expired", key)
		}

		// 如果数据被 Watch，则通知所有订阅者
		if s.HaveWatch(key) {
			s.Notify(&Notify{
				WT:    WT_MOVE_EXPIRE,
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
	caKey := s.ParseKey(key, false)

	err = s.db.UpdateFunc(caKey.Key, caKey.Bucket, func(bkt *bbolt.Bucket, v *KVEntry) error {
		entry = v
		// 设置 TTL
		v.TTL = time.Now().Unix() + ttl
		s.expireIndex.Add(key, v.TTL)

		// 如果数据被 Watch，则通知所有订阅者
		if s.HaveWatch(key) {
			s.Notify(&Notify{
				WT:    WT_ADD_EXPIRE,
				Key:   key,
				Entry: v,
			})
		}

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

// Lock
// 锁
func (s *KVStore) Lock(clientId string, key string, ttl int64) (entry *LockKVEntry, err error) {
	caKey := s.ParseKey(key, true)

	entry, err = s.lockdb.Get(caKey.Bucket, caKey.Key)
	if err != nil && err.Error() != "EOF" {
		return
	}

	// 如果存在，则判断是否是同一个客户端，如果是则更新 TTL
	if entry != nil && entry.Value == clientId {
		if entry.Lock {
			return nil, fmt.Errorf("key %s is locked by client %s", key, clientId)
		}
	}

	entry = &LockKVEntry{
		Lock:    true,
		Value:   clientId,
		Key:     key,
		TTL:     ttl,
		LeaseID: pkgUtils.GenID(),
	}

	err = s.lockdb.Put(caKey.Bucket, caKey.Key, entry)
	return
}

// Unlock
// 解锁
func (s *KVStore) Unlock(clientId string, LeaseID int64, key string) (err error) {
	// 将对应的key lock false ，value 制空
	caKey := s.ParseKey(key, true)
	// 查询是否存在，如果存在需要对比是否是同一个客户端，如果不是则返回错误
	entry, err := s.lockdb.Get(caKey.Bucket, caKey.Key)
	if err != nil {
		return
	}

	// 如果不是同一个客户端，则返回错误
	if entry.LeaseID != LeaseID {
		return fmt.Errorf("key %s is not locked by client %s", key, clientId)
	}

	if entry.Value != clientId {
		return fmt.Errorf("key %s is not locked by client %s", key, clientId)
	}

	return s.lockdb.Put(caKey.Bucket, caKey.Key, &LockKVEntry{Lock: false, Value: ""})
}

// TryLock
// 尝试加锁
func (s *KVStore) TryLock(clientId string, key string, ttl int64) (entry *LockKVEntry, err error) {
	// 将对应的key lock false ，value 制空
	caKey := s.ParseKey(key, true)
	// 查询是否存在，如果存在需要对比是否是同一个客户端，如果不是则返回错误
	entry, err = s.lockdb.Get(caKey.Bucket, caKey.Key)
	if err != nil {
		if err.Error() == "EOF" {
			err = nil
		} else {
			return
		}
	}

	// 如果不存在，则直接加锁
	if entry == nil {
		entry = &LockKVEntry{Key: key, TTL: ttl, Lock: true, Value: clientId, LeaseID: pkgUtils.GenID()}
		err = s.lockdb.Put(caKey.Bucket, caKey.Key, entry)
		return
	}

	// 如果当前锁被占用，则返回错误
	if entry.Lock {
		return nil, fmt.Errorf("key %s is locked by client %s", key, entry.Value)
	}

	// 如果 entry value 为空，则直接加锁
	if entry.Value == "" {
		entry = &LockKVEntry{Key: key, TTL: ttl, Lock: true, Value: clientId, LeaseID: pkgUtils.GenID()}
		err = s.lockdb.Put(caKey.Bucket, caKey.Key, entry)
		return
	}

	// 如果存在，则判断是否是同一个客户端，如果是则更新 TTL
	if entry.Value == clientId {
		entry.TTL = time.Now().Unix() + ttl
		err = s.lockdb.Put(caKey.Bucket, caKey.Key, entry)
		return
	}

	// 如果不是同一个客户端，则返回错误
	if entry.Value != clientId {
		return nil, fmt.Errorf("key %s is locked by client %s", key, entry.Value)
	}

	return
}

// 续约锁
func (s *KVStore) RenewLock(clientId string, key string, LeaseID int64, ttl int64) (entry *LockKVEntry, err error) {
	caKey := s.ParseKey(key, true)
	// 查询是否存在，如果存在需要对比是否是同一个客户端，如果不是则返回错误
	entry, err = s.lockdb.Get(caKey.Bucket, caKey.Key)
	if err != nil {
		return
	}

	// 如果不是同一个客户端，则返回错误
	if entry.Value != clientId {
		return nil, fmt.Errorf("key %s is not locked by client %s", key, clientId)
	}

	// 如果不是同一个续约ID，则返回错误
	if entry.LeaseID != LeaseID {
		return nil, fmt.Errorf("key %s is not locked by leaseID %d", key, LeaseID)
	}

	// 如果存在，则判断是否是同一个客户端，如果是则更新 TTL
	if entry.Value == clientId {
		entry.TTL = time.Now().Unix() + ttl
		err = s.lockdb.Put(caKey.Bucket, caKey.Key, entry)
	}

	return
}
