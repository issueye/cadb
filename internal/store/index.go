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
	Value     string    `json:"value"`      // key 的值
	TTL       int64     `json:"ttl"`        // 过期时间
	Key       string    `json:"key"`        // key
	CreatedAt time.Time `json:"created_at"` // 创建时间
	UpdatedAt time.Time `json:"updated_at"` // 更新时间
	ExpireAt  time.Time `json:"expire_at"`  // 过期时间
	Revision  int64     `json:"revision"`   // 版本号
}

// 用于锁定 key 的结构体
type LockKVEntry struct {
	*KVEntry
	Lock    bool  `json:"lock"`     // 是否被锁定
	LeaseID int64 `json:"lease_id"` // 租约ID
}

func NewLockKVEntry(key string, value string, ttl int64) *LockKVEntry {
	now := time.Now()
	return &LockKVEntry{
		Lock:    true,
		LeaseID: pkgUtils.GenID(),
		KVEntry: &KVEntry{
			Key:       key,
			Value:     value,
			TTL:       now.Add(time.Duration(ttl) * time.Second).Unix(),
			CreatedAt: now,
			UpdatedAt: now,
			ExpireAt:  now.Add(time.Duration(ttl) * time.Second),
			Revision:  0,
		},
	}
}

type KVStore struct {
	// bolt 数据库
	db *DB[KVEntry]
	// bolt 数据库
	lockdb *DB[LockKVEntry]
	// 过期索引
	expireIndex *ExpireIndex
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
		watchedKeys:     make(map[string][]*Watcher),
		watchedKeysLock: &sync.Mutex{},
	}

	kv.expireIndex = NewExpireIndex(kv)

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
	ok, err := s.Client.CheckClientSecretKeyExists(secretKey)
	return ok && err == nil
}

// Set 设置指定 key 的值
func (s *KVStore) Set(key, value string, ttl int64) error {
	caKey := s.ParseKey(key, false)

	entry := &KVEntry{
		Key:   key,
		TTL:   ttl,
		Value: value,
	}

	// 设置过期时间
	if ttl > 0 {
		now := time.Now()

		entry.TTL = now.Add(time.Duration(ttl) * time.Second).Unix()
		entry.ExpireAt = now.Add(time.Duration(ttl) * time.Second)
		entry.CreatedAt = now
		entry.UpdatedAt = now
		s.expireIndex.Add(key, entry.TTL)
	}

	// 将数据写入 BoltDB
	err := s.db.Put(caKey.Bucket, caKey.Key, entry)
	if err != nil {
		return err
	}

	// 存储一份 key 到 keysBucket 中
	err = s.db.db.Update(func(tx *bbolt.Tx) error { return tx.Bucket([]byte(keysBucket)).Put([]byte(key), []byte{}) })
	if err != nil {
		return err
	}

	// 通知所有订阅了该 key 的订阅者
	Notify(WT_PUT, &Notification{
		Key:   key,
		Entry: NewFromEntry(entry),
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

	// 从过期缓存中移除数据
	s.expireIndex.Remove(key)

	// 如果数据被 Watch，则通知所有订阅者
	if HaveWatch(key) {
		Notify(WT_DELETE, &Notification{
			Key:   key,
			Entry: NewFromEntry(entry),
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
		if HaveWatch(key) {
			Notify(WT_MOVE_EXPIRE, &Notification{
				Key:   key,
				Entry: NewFromEntry(v),
			})
		}

		// 移除过期缓存中的数据
		s.expireIndex.Remove(key)
		v.TTL = 0
		v.ExpireAt = time.Time{}
		v.UpdatedAt = time.Now()
		v.Revision += 1

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
		now := time.Now()
		v.TTL = now.Add(time.Duration(ttl) * time.Second).Unix()
		s.expireIndex.Add(key, v.TTL)
		v.ExpireAt = now.Add(time.Duration(ttl) * time.Second)
		v.UpdatedAt = time.Now()
		v.Revision += 1

		// 如果数据被 Watch，则通知所有订阅者
		if HaveWatch(key) {
			Notify(WT_ADD_EXPIRE, &Notification{
				Key:   key,
				Entry: NewFromEntry(v),
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
