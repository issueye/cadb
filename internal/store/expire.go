package store

import (
	"log"
	"sort"
	"sync"
	"time"
)

type Expire struct {
	ExpireAt int64  // 过期时间戳
	Key      string // 数据的键
	IsLock   bool   // 是否是锁
}

type ExpireIndex struct {
	expireList []Expire      // 根据过期时间进行排序的有序列表
	lock       *sync.RWMutex // 读写锁
}

// NewExpireIndex
// 创建一个新的过期索引
func NewExpireIndex() *ExpireIndex {
	return &ExpireIndex{
		lock: &sync.RWMutex{},
	}
}

type ExpireLoop struct {
	Key    string
	IsLock bool
}

// getExpiredKeys
// 获取所有即将过期的键
// 参数：now 当前时间戳
// 返回值：即将过期的键列表
// 错误：如果发生错误，则返回错误信息
func (ei *ExpireIndex) getExpiredKeys(now int64) ([]*ExpireLoop, error) {
	ei.lock.RLock()
	defer ei.lock.RUnlock()

	// 因为数据已经根据过期时间排序，所以只需要遍历过期时间小于等于当前时间的元素即可
	keys := make([]*ExpireLoop, 0)

	if len(ei.expireList) == 0 {
		return keys, nil
	}

	for _, expire := range ei.expireList {
		// 当前的元素未过期，后续的元素也不会过期，直接退出循环
		if expire.ExpireAt > now {
			break
		}

		keys = append(keys, &ExpireLoop{Key: expire.Key, IsLock: expire.IsLock})
	}

	return keys, nil
}

// add
// 添加一个键到过期索引中
// 参数：key 键
// expireAt 过期时间戳
func (ei *ExpireIndex) Add(key string, expireAt int64) {
	ei.lock.Lock()

	func() {
		defer ei.lock.Unlock()
		// 防止重复添加
		for _, expire := range ei.expireList {
			if expire.Key == key {
				// 如果过期时间更短，则更新过期时间
				if expire.ExpireAt > expireAt {
					expire.ExpireAt = expireAt
				}
				return
			}
		}
		// 将键添加到过期索引中
		ei.expireList = append(ei.expireList, Expire{ExpireAt: expireAt, Key: key})
	}()

	// 对过期索引进行排序
	ei.Sort()
}

// remove
// 从过期索引中删除一个键
// 参数：key 键
func (ei *ExpireIndex) Remove(key string) {
	ei.lock.Lock()
	defer ei.lock.Unlock()
	// 从过期索引中删除键
	for i, expire := range ei.expireList {
		if expire.Key == key {
			ei.expireList = append(ei.expireList[:i], ei.expireList[i+1:]...)
			break
		}
	}
}

// Sort // 对过期索引进行排序
func (ei *ExpireIndex) Sort() {
	ei.lock.Lock()
	defer ei.lock.Unlock()
	sort.Slice(ei.expireList, func(i, j int) bool {
		return ei.expireList[i].ExpireAt < ei.expireList[j].ExpireAt
	})
}

func (s *KVStore) startExpireLoop() {
	// 避免协程的数据竞争，每次只处理一个 goroutine 的数据
	getKeys := func() []*ExpireLoop {
		s.watchedKeysLock.Lock()
		defer s.watchedKeysLock.Unlock()
		// 从过期时间索引中获取即将过期的数据
		keys, err := s.expireIndex.getExpiredKeys(time.Now().Unix())
		if err != nil {
			log.Printf("Error getting expired keys: %v", err)
			return nil
		}

		return keys
	}

	// 启动多个 goroutine 处理过期数据
	go func() {
		for {
			keys := getKeys()

			// 检查 key 是否被 Watch，如果被 Watch，则通知
			// 如果 key 的数量大于 100，则分成多个 goroutine 处理
			for _, key := range keys {
				have := HaveWatch(key.Key)
				if have {
					// 获取数据
					msg := New()
					msg.Key = key.Key

					s.SendNotify(key.Key, key.IsLock)
				}

				s.expireIndex.Remove(key.Key)
			}

			// 短暂休眠,减轻系统负载
			time.Sleep(time.Second)
		}
	}()
}

func (s *KVStore) SendNotify(key string, isLock bool) {
	var CaKey *CaKey
	if isLock {
		CaKey = s.ParseKey(key, true)
		data, err := s.lockdb.Get(CaKey.Bucket, CaKey.Key)
		if err != nil {
			log.Printf("Error getting lock: %v", err)
			return
		}

		Notify(WT_LOCK_EXPIRE, &Notification{
			Key:    key,
			IsLock: isLock,
			Entry:  NewFromLock(data),
		})

		err = s.lockdb.Delete(CaKey.Bucket, CaKey.Key)
		if err != nil {
			log.Printf("Error deleting lock: %v", err)
			return
		}
	} else {
		CaKey = s.ParseKey(key, false)

		data, err := s.db.Get(CaKey.Bucket, CaKey.Key)
		if err != nil {
			log.Printf("Error getting key: %v", err)
			return
		}

		Notify(WT_EXPIRE, &Notification{
			Key:    key,
			IsLock: isLock,
			Entry:  NewFromEntry(data),
		})

		err = s.db.Delete(CaKey.Bucket, CaKey.Key)
		if err != nil {
			log.Printf("Error deleting key: %v", err)
			return
		}
	}
}
