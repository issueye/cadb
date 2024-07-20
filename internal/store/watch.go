package store

import "github.com/google/uuid"

type WType int

const (
	WT_Put          WType = 0
	WT_Del          WType = 1
	WT_Expire       WType = 2
	WT_ModifyExpire WType = 3
)

type Notify struct {
	Key   string
	WT    WType
	Entry *KVEntry
}

type WatchCallback func(WT WType, entry *KVEntry)

// Watcher
// 观察者
type Watcher struct {
	Id       string        // 观察者ID
	CallBack WatchCallback // 回调函数
}

// IsWatch
// 判断指定 key 是否被监听
func (s *KVStore) HaveWatch(key string) bool {
	s.watchedKeysLock.Lock()
	defer s.watchedKeysLock.Unlock()

	_, ok := s.watchedKeys[key]
	return ok
}

// Watch
// 监听指定 key 的变化,并在有变化时通知订阅者
func (s *KVStore) Watch(key string, cb WatchCallback) (id string, err error) {
	s.watchedKeysLock.Lock()
	defer s.watchedKeysLock.Unlock()

	// 将 key 和对应的 channel 添加到 watchedKeys 中
	watcher := &Watcher{Id: uuid.New().String(), CallBack: cb}
	s.watchedKeys[key] = append(s.watchedKeys[key], watcher)
	return watcher.Id, nil
}

// MoveWatch
// 取消监听指定 key 的变化
// 如果没有订阅者了,则将 key 从 watchedKeys 中移除
func (s *KVStore) MoveWatch(key string, id string) {
	s.watchedKeysLock.Lock()
	defer s.watchedKeysLock.Unlock()

	// 将 key 和对应的 channel 从 watchedKeys 中移除
	for i, w := range s.watchedKeys[key] {
		if w.Id == id {
			s.watchedKeys[key] = append(s.watchedKeys[key][:i], s.watchedKeys[key][i+1:]...)

			// 如果该 key 没有订阅者了,则将 key 从 watchedKeys 中移除
			if len(s.watchedKeys[key]) == 0 {
				delete(s.watchedKeys, key)
			}
			break
		}
	}
}

// Notify
// 通知所有订阅了指定 key 的订阅者
func (s *KVStore) Notify(data *Notify) {
	s.watchedKeysLock.Lock()
	defer s.watchedKeysLock.Unlock()

	// 通知所有订阅了该 key 的订阅者
	for _, w := range s.watchedKeys[data.Key] {
		go w.CallBack(data.WT, data.Entry)
	}
}
