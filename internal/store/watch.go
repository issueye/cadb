package store

import (
	"encoding/json"
	"sync"
)

type WType int

const (
	WT_PUT         WType = iota // 添加
	WT_DELETE                   // 删除
	WT_EXPIRE                   // 过期
	WT_MOVE_EXPIRE              // 移除过期
	WT_ADD_EXPIRE               // 添加过期
	WT_LOCK                     // 锁
	WT_UNLOCK                   // 解锁
	WT_TRYLOCK                  // 尝试加锁
	WT_RENEWLOCK                // 续约
	WT_LOCK_EXPIRE              // 锁过期
)

type Message struct {
	Key     string `json:"key"`     // 键
	Value   string `json:"value"`   // 值
	Expire  int64  `json:"expire"`  // 过期时间
	Op      WType  `json:"op"`      // 操作类型
	IsLock  bool   `json:"islock"`  // 锁
	LeaseID int64  `json:"leaseid"` // 租约ID
}

func (msg *Message) CopyFromEntry(entry *KVEntry) {
	msg.Key = entry.Key
	msg.Value = entry.Value
	msg.Expire = entry.TTL
	msg.IsLock = false
	msg.LeaseID = 0
}

func (msg *Message) CopyFromLock(entry *LockKVEntry) {
	msg.Key = entry.Key
	msg.Value = entry.Value
	msg.Expire = entry.TTL
	msg.IsLock = entry.Lock
	msg.LeaseID = entry.LeaseID
}

func NewFromEntry(entry *KVEntry) *Message {
	return &Message{
		Key:     entry.Key,
		Value:   entry.Value,
		Expire:  entry.TTL,
		Op:      WT_PUT,
		IsLock:  false,
		LeaseID: -1,
	}
}

func New() *Message {
	return &Message{}
}

func NewFromLock(entry *LockKVEntry) *Message {
	return &Message{
		Key:     entry.Key,
		Value:   "************", // 不显示客户端的ID
		Expire:  entry.TTL,
		Op:      WT_PUT,
		IsLock:  entry.Lock,
		LeaseID: entry.LeaseID,
	}
}

type Notification struct {
	Key    string   `json:"key"`     // 建
	IsLock bool     `json:"is_lock"` // 锁
	Entry  *Message `json:"entry"`   // 数据
}

func (notify *Notification) ToString() string {
	// 返回一个 json 字符串
	data, err := json.Marshal(notify)
	if err != nil {
		return ""
	}

	return string(data)
}

type WatchCallback func(WT WType, entry *Notification)

// Watcher
// 观察者
type Watcher struct {
	Id       string        // 观察者ID
	CallBack WatchCallback // 回调函数
	Close    chan struct{} // 关闭信号
}

// watchedKeys
type Observer struct {
	watchedKeysLock *sync.RWMutex
	Watchers        map[string][]*Watcher
	WatcherLocks    map[string][]*Watcher
}

func NewObserver() *Observer {
	return &Observer{
		Watchers:        make(map[string][]*Watcher),
		WatcherLocks:    make(map[string][]*Watcher),
		watchedKeysLock: &sync.RWMutex{},
	}
}

var ObserverInstance = NewObserver()

// CheckWatch
// 检查指定 key 是否被监听
func (o *Observer) CheckWatch(key string, id string, isLock bool) *Watcher {
	o.watchedKeysLock.RLock()
	defer o.watchedKeysLock.RUnlock()

	var watchers []*Watcher
	if isLock {
		watchers = o.WatcherLocks[key]
	} else {
		watchers = o.Watchers[key]
	}

	for _, w := range watchers {
		if w.Id == id {
			return w
		}
	}

	return nil
}

// AddWatch
// 添加观察者
func (o *Observer) AddWatch(key string, watcher *Watcher, isLock bool) *Watcher {
	if w := o.CheckWatch(key, watcher.Id, isLock); w != nil {
		// 覆盖原有 watcher
		w.Close <- struct{}{}
		close(w.Close)
		watcher.Close = make(chan struct{})

		w = watcher
		return w
	}

	o.watchedKeysLock.Lock()
	defer o.watchedKeysLock.Unlock()

	var watchers []*Watcher
	if isLock {
		watchers = o.WatcherLocks[key]
	} else {
		watchers = o.Watchers[key]
	}

	// 检查 watchers 是否存在
	if watchers == nil {
		if isLock {
			o.WatcherLocks[key] = make([]*Watcher, 0)
		} else {
			o.Watchers[key] = make([]*Watcher, 0)
		}
	}

	// 添加 watcher
	watchers = append(watchers, watcher)
	return watcher
}

// RemoveWatch
// 移除观察者
func (o *Observer) RemoveWatch(key string, id string, isLock bool) {
	// 检查Wacher 是否存在
	if wacther := o.CheckWatch(key, id, isLock); wacther != nil {
		o.watchedKeysLock.Lock()
		defer o.watchedKeysLock.Unlock()
		var watchers []*Watcher
		if isLock {
			watchers = o.WatcherLocks[key]
		} else {
			watchers = o.Watchers[key]
		}
		for i, w := range watchers {
			if w.Id == id {
				watchers = append(watchers[:i], watchers[i+1:]...)
				close(w.Close)
				return
			}
		}
	}
}

// SendNotification
// 发送通知
func (o *Observer) SendNotification(key string, wt WType, notify *Notification) {
	o.watchedKeysLock.RLock()
	defer o.watchedKeysLock.RUnlock()

	var watchers []*Watcher
	if notify.IsLock {
		watchers = append(watchers, o.WatcherLocks[key]...)
	} else {
		watchers = append(watchers, o.Watchers[key]...)
	}

	for _, w := range watchers {
		go w.CallBack(wt, notify)
	}
}

// HaveWatch
// 判断指定 key 是否被监听
func HaveWatch(key string) bool {
	ObserverInstance.watchedKeysLock.RLock()
	defer ObserverInstance.watchedKeysLock.RUnlock()
	return len(ObserverInstance.Watchers[key]) > 0
}

// Watch
// 监听指定 key 的变化,并在有变化时通知订阅者

func Watch(id string, key string, cb WatchCallback) *Watcher {
	watcher := &Watcher{Id: id, CallBack: cb, Close: make(chan struct{})}
	return ObserverInstance.AddWatch(key, watcher, false)
}

func CheckWatch(id string, key string, isLock bool) *Watcher {
	return ObserverInstance.CheckWatch(key, id, isLock)
}

// RemoveWatch
// 取消监听指定 key 的变化
// 如果没有订阅者了,则将 key 从 watchedKeys 中移除
func RemoveWatch(key string, id string) {
	ObserverInstance.RemoveWatch(key, id, false)
}

// Notify
// 通知所有订阅了指定 key 的订阅者
func Notify(wt WType, data *Notification) {
	ObserverInstance.SendNotification(data.Key, wt, data)
}

// WatchLock
// 监听指定 key 的变化,并在有变化时通知订阅者
func WatchLock(id string, key string, cb WatchCallback) *Watcher {
	watcher := &Watcher{Id: id, CallBack: cb, Close: make(chan struct{})}
	return ObserverInstance.AddWatch(key, watcher, true)
}

// RemoveWatchLock 取消监听指定 key 的变化
// 如果没有订阅者了,则将 key 从 watchedKeys 中移除
func RemoveWatchLock(key string, id string) {
	ObserverInstance.RemoveWatch(key, id, true)
}
