package store

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type Expire struct {
	ExpireAt int64  // 过期时间戳
	Key      string // 数据的键
	IsLock   bool   // 是否是锁
}

type ExpireList struct {
	list  []*Expire
	lock  *sync.RWMutex
	store *KVStore
}

func NewExpireList(store *KVStore) *ExpireList {
	return &ExpireList{
		list:  make([]*Expire, 0),
		lock:  &sync.RWMutex{},
		store: store,
	}
}

// Len
// 长度
func (el *ExpireList) Len() int {
	return len(el.list)
}

// Sort
// 排序
func (el *ExpireList) Sort() {
	// 加锁
	el.lock.Lock()
	defer el.lock.Unlock()

	sort.Slice(el.list, func(i, j int) bool {
		return el.list[i].ExpireAt < el.list[j].ExpireAt
	})
}

// GetKey
// 获取Key
func (el *ExpireList) GetKey(key string) (expire *Expire) {
	el.lock.RLock()
	defer el.lock.RUnlock()

	for _, e := range el.list {
		if e.Key == key {
			expire = e
			break
		}
	}

	return
}

// 获取所有过期的键
func (el *ExpireList) GetExpiredKeys(now int64) (keys []Expire) {
	el.lock.RLock()
	defer el.lock.RUnlock()

	for _, e := range el.list {
		if e.ExpireAt <= now {
			keys = append(keys, Expire{
				Key:      e.Key,
				ExpireAt: e.ExpireAt,
				IsLock:   e.IsLock,
			})
		}
	}
	return
}

// Add
// 添加一个元素
func (el *ExpireList) Add(expire *Expire) (err error) {
	el.lock.Lock()
	func() {
		defer el.lock.Unlock()

		// 判断是否存在
		for _, e := range el.list {
			if e.Key == expire.Key {
				err = fmt.Errorf("key %s already exists", expire.Key)
				return
			}
		}
	}()

	el.list = append(el.list, expire)
	el.Sort()
	return
}

// Remove
// 删除一个元素
func (el *ExpireList) Remove(key string) (err error) {
	el.lock.Lock()
	defer el.lock.Unlock()

	for i, e := range el.list {
		if e.Key == key {
			el.list = append(el.list[:i], el.list[i+1:]...)
			return
		}
	}

	err = fmt.Errorf("key %s not found", key)
	return
}

// RenewExpire
// 更新过期时间
func (el *ExpireList) RenewExpire(key string, expireAt int64) (err error) {
	el.lock.Lock()

	// 避免锁竞争，因为更新过期时间可能会影响排序
	func() {
		defer el.lock.Unlock()

		for _, e := range el.list {
			if e.Key == key {
				e.ExpireAt = expireAt
				return
			}
		}
	}()

	el.Sort()
	err = fmt.Errorf("key %s not found", key)
	return
}

func (el *ExpireList) startExpireLoop() {
	go func() {
		for {
			now := time.Now().Unix()
			keys := el.GetExpiredKeys(now)

			if len(keys) > 0 {
				for _, key := range keys {
					el.expire(key)
				}
			}

			time.Sleep(time.Second)
		}
	}()
}

func (el *ExpireList) expire(key Expire) {

	var (
		msg *Message
	)

	caKey := el.store.ParseKey(key.Key, key.IsLock)
	if key.IsLock {
		entry, err := el.store.lockdb.Get(caKey.Bucket, caKey.Key)
		if err != nil {
			return
		}
		msg = NewFromLock(entry)

		// 删除数据
		el.store.lockdb.Delete(caKey.Bucket, caKey.Key)
	} else {
		entry, err := el.store.db.Get(caKey.Bucket, caKey.Key)
		if err != nil {
			return
		}
		msg = NewFromEntry(entry)

		// 删除数据
		el.store.db.Delete(caKey.Bucket, caKey.Key)
	}

	Notify(
		WT_EXPIRE,
		&Notification{
			Key:    key.Key,
			IsLock: key.IsLock,
			Entry:  msg,
		},
	)

	el.Remove(key.Key)
}

type ExpireIndex struct {
	expireList        *ExpireList
	expireListForLock *ExpireList
}

// NewExpireIndex
// 创建一个新的过期索引
func NewExpireIndex(store *KVStore) *ExpireIndex {
	index := &ExpireIndex{
		expireList:        NewExpireList(store),
		expireListForLock: NewExpireList(store),
	}

	index.expireList.startExpireLoop()
	index.expireListForLock.startExpireLoop()
	return index
}

// AddLock
// 添加一个锁到过期索引中
// 参数：key 键
// expireAt 过期时间戳
func (ei *ExpireIndex) AddLock(key string, expireAt int64) (err error) {
	return ei.expireListForLock.Add(&Expire{ExpireAt: expireAt, Key: key, IsLock: true})
}

// RemoveLock
// 从过期索引中删除一个锁
// 参数：key 键
func (ei *ExpireIndex) RemoveLock(key string) error { return ei.expireListForLock.Remove(key) }

// RenewExpire
// 更新一个键的过期时间
// 参数：key 键
// expireAt 新的过期时间戳
func (ei *ExpireIndex) RenewLock(key string, expireAt int64) error {
	return ei.expireListForLock.RenewExpire(key, expireAt)
}

// add
// 添加一个键到过期索引中
// 参数：key 键
// expireAt 过期时间戳
func (ei *ExpireIndex) Add(key string, expireAt int64) (err error) {
	return ei.expireList.Add(&Expire{ExpireAt: expireAt, Key: key})
}

// remove
// 从过期索引中删除一个键
// 参数：key 键
func (ei *ExpireIndex) Remove(key string) error {
	return ei.expireList.Remove(key)
}
