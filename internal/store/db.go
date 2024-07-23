package store

import (
	"go.etcd.io/bbolt"
	"golang.corp.yxkj.com/orange/cadb/internal/utils"
)

type DB[T any] struct {
	db *bbolt.DB
}

// NewDB
// 创建数据库
func NewDB[T any](path string) (*DB[T], error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	return &DB[T]{db: db}, nil
}

func (db *DB[T]) Close() error { return db.db.Close() }

// Update
// 更新数据
func (db *DB[T]) Update(fn func(tx *bbolt.Tx) error) error { return db.db.Update(fn) }

// UpdateFunc
// 更新数据
func (db *DB[T]) UpdateFunc(key, bucket []byte, fn func(bkt *bbolt.Bucket, v *T) error) error {
	return db.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}

		data := b.Get(key)
		if data == nil {
			return nil
		}

		value := new(T)
		err := utils.UnMarshal(data, value)
		if err != nil {
			return err
		}

		return fn(b, value)
	})
}

// View
// 查询数据
func (db *DB[T]) View(fn func(tx *bbolt.Tx) error) error { return db.db.View(fn) }

// ViewFunc 查询数据
func (db *DB[T]) ViewFunc(key, bucket []byte, fn func(bkt *bbolt.Bucket, v *T) error) error {
	return db.View(func(tx *bbolt.Tx) error {
		return fn(tx.Bucket(bucket), nil)
	})
}

// Get
// 获取数据
func (db *DB[T]) Get(bucket, key []byte) (data *T, err error) {
	var v []byte
	err = db.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		v = b.Get(key)

		return utils.UnMarshal(v, &data)
	})
	return
}

// Put 写入数据
func (db *DB[T]) Put(bucket, key []byte, value *T) error {
	return db.db.Update(func(tx *bbolt.Tx) error {
		data := []byte{}
		if value != nil {
			data = utils.MustMarshal(value)
		}

		return tx.Bucket(bucket).Put(key, data)
	})
}

// PutEmpty 写入空数据
func (db *DB[T]) PutEmpty(bucket, key []byte) error { return db.Put(bucket, key, (*T)(nil)) }

func (db *DB[T]) Delete(bucket, key []byte) error {
	return db.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucket).Delete(key)
	})
}

// Delete 删除数据
func (db *DB[T]) DeleteR(bucket, key []byte) (data *T, err error) {
	data, err = db.Get(bucket, key)
	if err != nil {
		return nil, err
	}

	err = db.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucket).Delete(key)
	})

	return
}

// List 获取所有数据
func (db *DB[T]) List(bucket []byte) (data []*T, err error) {
	err = db.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var t T
			if err := utils.UnMarshal(v, &t); err != nil {
				return err
			}
			data = append(data, &t)
		}
		return nil
	})
	return
}

// ForEach 遍历数据
func (db *DB[T]) ForEach(bucket []byte, fn func(k []byte, v *T) (bk bool, err error)) error {
	return db.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}

		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var t T
			if err := utils.UnMarshal(v, &t); err != nil {
				return err
			}
			if bk, err := fn(k, &t); err != nil {
				return err
			} else {
				if bk {
					break
				}
			}
		}
		return nil
	})
}

// createBucket 创建bucket
func (db *DB[T]) CreateBucket(bucket []byte) error {
	return db.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucket)
		return err
	})
}
