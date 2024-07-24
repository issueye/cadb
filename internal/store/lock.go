package store

import (
	"fmt"
	"time"

	pkgUtils "golang.corp.yxkj.com/orange/cadb/pkg/utils"
)

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
		LeaseID: pkgUtils.GenID(),
		KVEntry: KVEntry{
			Value: clientId,
			Key:   key,
			TTL:   ttl,
		},
	}

	// 将数据添加到 过期检测中
	s.expireIndex.Add(key, time.Now().Add(time.Duration(ttl)*time.Second).Unix())
	err = s.lockdb.Put(caKey.Bucket, caKey.Key, entry)
	if err != nil {
		return
	}

	// 推送消息
	Notify(WT_LOCK, &Notification{
		Key:    key,
		IsLock: true,
		Entry:  NewFromLock(entry),
	})

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

	err = s.lockdb.Put(caKey.Bucket, caKey.Key, &LockKVEntry{
		Lock: false,
		KVEntry: KVEntry{
			Value: "",
		},
	})

	if err != nil {
		return
	}

	Notify(WT_UNLOCK, &Notification{
		Key:    key,
		IsLock: true,
		Entry:  NewFromLock(entry),
	})
	return
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

	defer func() {
		if err != nil {
			// 添加到过期检测中
			s.expireIndex.Add(key, time.Now().Add(time.Duration(ttl)*time.Second).Unix())
		}
	}()

	// 如果不存在，则直接加锁
	if entry == nil {
		entry = &LockKVEntry{
			Lock:    true,
			LeaseID: pkgUtils.GenID(),
			KVEntry: KVEntry{
				Key:   key,
				TTL:   ttl,
				Value: clientId,
			},
		}
		err = s.lockdb.Put(caKey.Bucket, caKey.Key, entry)
		if err != nil {
			return
		}

		// 推送
		Notify(WT_TRYLOCK, &Notification{Key: key, IsLock: true, Entry: NewFromLock(entry)})
		return
	}

	// 如果当前锁被占用，则返回错误
	if entry.Lock {
		err = fmt.Errorf("key %s is locked by client %s", key, entry.Value)
		return
	}

	// 如果 entry value 为空，则直接加锁
	if entry.Value == "" {
		entry = &LockKVEntry{
			Lock:    true,
			LeaseID: pkgUtils.GenID(),
			KVEntry: KVEntry{
				Key:   key,
				TTL:   ttl,
				Value: clientId,
			},
		}
		err = s.lockdb.Put(caKey.Bucket, caKey.Key, entry)
		if err != nil {
			return
		}

		// 推送
		Notify(WT_TRYLOCK, &Notification{Key: key, IsLock: true, Entry: NewFromLock(entry)})
		return
	}

	// 如果不是同一个客户端，则返回错误
	if entry.Value != clientId {
		err = fmt.Errorf("key %s is locked by client %s", key, entry.Value)
		return
	} else {
		// 如果存在，则判断是否是同一个客户端，如果是则更新 TTL
		entry.TTL = time.Now().Unix() + ttl
		err = s.lockdb.Put(caKey.Bucket, caKey.Key, entry)
		if err != nil {
			return
		}
	}

	// 推送
	Notify(WT_TRYLOCK, &Notification{Key: key, IsLock: true, Entry: NewFromLock(entry)})
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

	// 推送
	Notify(WT_RENEWLOCK, &Notification{Key: key, IsLock: true, Entry: NewFromLock(entry)})
	return
}
