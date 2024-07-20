package store

import (
	"fmt"
	"log"
	"time"

	"go.etcd.io/bbolt"
)

// Snapshot
// 针对数据库的快照
func (s *KVStore) Snapshot() {
	// 每十分钟进行一次快照，使用定时器
	ticker := time.NewTicker(10 * time.Minute)
	for range ticker.C {
		// 快照文件名称
		snapshotFile := fmt.Sprintf("snapshot-%d.db", time.Now().Unix())
		// 创建一个 boolt 数据库
		snapshotDB, err := bbolt.Open(snapshotFile, 0600, nil)
		if err != nil {
			log.Fatalf("failed to create snapshot db: %v", err)
		}

		defer snapshotDB.Close()

		s.db.View(func(tx *bbolt.Tx) error {
			return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
				return snapshotDB.Update(func(stx *bbolt.Tx) error {
					snapshotBucket, err := stx.CreateBucket(name)
					if err != nil {
						return err
					}
					return b.ForEach(func(k, v []byte) error {
						return snapshotBucket.Put(k, v)
					})
				})
			})
		})
	}
}
