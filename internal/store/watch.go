package store

type WType int

const (
	WT_PUT         WType = iota // 添加
	WT_DELETE                   // 删除
	WT_EXPIRE                   // 过期
	WT_MOVE_EXPIRE              // 移除过期
	WT_ADD_EXPIRE               // 添加过期
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
	Close    chan struct{} // 关闭信号
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
func (s *KVStore) Watch(id string, key string, cb WatchCallback) *Watcher {
	s.watchedKeysLock.Lock()
	defer s.watchedKeysLock.Unlock()

	// 将 key 和对应的 channel 添加到 watchedKeys 中
	watcher := &Watcher{Id: id, CallBack: cb, Close: make(chan struct{})}
	s.watchedKeys[key] = append(s.watchedKeys[key], watcher)

	return watcher
}

func (s *KVStore) CheckWatch(id string, key string) *Watcher {
	s.watchedKeysLock.Lock()
	defer s.watchedKeysLock.Unlock()

	for _, w := range s.watchedKeys[key] {
		if w.Id == id {
			return w
		}
	}
	return nil
}

// RemoveWatch
// 取消监听指定 key 的变化
// 如果没有订阅者了,则将 key 从 watchedKeys 中移除
func (s *KVStore) RemoveWatch(key string, id string) {
	s.watchedKeysLock.Lock()
	defer s.watchedKeysLock.Unlock()

	// 将 key 和对应的 channel 从 watchedKeys 中移除
	for i, w := range s.watchedKeys[key] {
		if w.Id == id {
			s.watchedKeys[key] = append(s.watchedKeys[key][:i], s.watchedKeys[key][i+1:]...)
			close(w.Close)
			// 如果该 key 没有订阅者了,则将 key 从 watchedKeys 中移除
			// if len(s.watchedKeys[key]) == 0 {
			// 	delete(s.watchedKeys, key)
			// }
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
	// fmt.Println("Notify key: ", data.Key)
	for _, w := range s.watchedKeys[data.Key] {
		go w.CallBack(data.WT, data.Entry)
	}
}
