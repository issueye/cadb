package store

import (
	"log"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"go.etcd.io/bbolt"
)

type Expire struct {
	ExpireAt int64  // 过期时间戳
	Key      string // 数据的键
}

type ExpireIndex struct {
	// 根据过期时间进行排序的有序列表
	expireList []Expire
	// 读写锁
	lock *sync.RWMutex
}

// NewExpireIndex
// 创建一个新的过期索引
func NewExpireIndex() *ExpireIndex {
	return &ExpireIndex{
		lock: &sync.RWMutex{},
	}
}

// getExpiredKeys
// 获取所有即将过期的键
// 参数：now 当前时间戳
// 返回值：即将过期的键列表
// 错误：如果发生错误，则返回错误信息
func (ei *ExpireIndex) getExpiredKeys(now int64) ([]string, error) {
	ei.lock.RLock()
	defer ei.lock.RUnlock()

	// 因为数据已经根据过期时间排序，所以只需要遍历过期时间小于等于当前时间的元素即可
	keys := make([]string, 0)
	for _, expire := range ei.expireList {
		// 如果当前的元素已经过期，则将其添加到结果列表中
		if expire.ExpireAt <= now {
			keys = append(keys, expire.Key)
		}

		// 当前的元素未过期，后续的元素也不会过期，直接退出循环
		if expire.ExpireAt > now {
			break
		}
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
	// 启动多个 goroutine 处理过期数据
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			for {
				// 从过期时间索引中获取即将过期的数据
				keys, err := s.expireIndex.getExpiredKeys(time.Now().Unix())
				if err != nil {
					log.Printf("Error getting expired keys: %v", err)
					continue
				}

				// 检查 key 是否被 Watch，如果被 Watch，则通知
				for _, key := range keys {
					have := s.HaveWatch(key)
					if have {
						// 获取数据
						s.Notify(&Notify{
							WT:  WT_EXPIRE,
							Key: key,
							Entry: &KVEntry{
								Key: key,
							},
						})
					}

					s.expireIndex.Remove(key)
				}

				// 批量删除过期数据
				s.batchDelete(keys)

				// 短暂休眠,减轻系统负载
				time.Sleep(time.Second)
			}
		}()
	}
}

func (s *KVStore) batchDelete(keys []string) {
	s.db.Update(func(tx *bbolt.Tx) error {
		for _, key := range keys {
			parts := strings.Split(key, "/")
			b := tx.Bucket([]byte(parts[0]))
			if b != nil {
				b.Delete([]byte(strings.Join(parts[1:], "/")))

				keysBucket := tx.Bucket([]byte(keysBucket))
				keysBucket.Delete([]byte(key))
			}
		}
		return nil
	})
}
