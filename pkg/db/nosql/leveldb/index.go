package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"golang.corp.yxkj.com/orange/cadb/internal/global"
	"golang.corp.yxkj.com/orange/cadb/pkg/utils"
)

// Index is an index of a leveldb database.
type Index[T any] struct {
	db *leveldb.DB
}

// NewIndex creates a new index.
func NewIndex[T any](path string) *Index[T] {
	// 存储数据库的路径
	ldb, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil
	}

	return &Index[T]{
		db: ldb,
	}
}

// Get returns the value for the given key.
func (idx *Index[T]) Get(key []byte) (*T, error) {
	// 获取数据
	value, err := idx.db.Get(key, nil)
	if err != nil {
		return nil, err
	}

	// 返回数据
	data := new(T)
	err = utils.BytesToStruct(value, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Put stores the given key-value pair.
func (idx *Index[T]) Put(key string, value *T) error {
	// 添加数据
	data, err := utils.StructToBytes(value)
	if err != nil {
		return err
	}

	err = idx.db.Put([]byte(key), data, nil)
	if err != nil {
		return err
	}

	return nil
}

// 通过索引删除数据
func (idx *Index[T]) Delete(key string) error {
	// 删除数据
	err := idx.db.Delete([]byte(key), nil)
	if err != nil {
		return err
	}

	return nil
}

// 关闭数据库
func (idx *Index[T]) Close() error {
	return idx.db.Close()
}

// Foreach
func (idx *Index[T]) Foreach(fn func(key string, value *T) error) error {
	// 获取迭代器
	iter := idx.db.NewIterator(nil, nil)
	defer iter.Release()

	// 遍历数据
	for iter.Next() {
		// 获取键值对
		key := iter.Key()
		value := iter.Value()

		// 返回数据
		data := new(T)
		err := utils.BytesToStruct(value, data)
		if err != nil {
			global.Log.Error("数据转换错误")
			continue
		}

		// 调用回调函数
		err = fn(string(key), data)
		if err != nil {
			return err
		}
	}

	return nil
}

// 根据前缀查找数据集，并且分页返回数据
func (idx *Index[T]) FindByPrefix(prefix string, page, pageSize int) ([]*T, error) {
	// 构建查询范围
	start := append([]byte(prefix), 0)
	limit := append([]byte(prefix), byte(0xFF))

	// 获取一个带有前缀的迭代器 带有布隆过滤器
	iter := idx.db.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
	defer iter.Release()

	// 初始化结果集
	results := make([]*T, 0)

	// 遍历数据
	for i := 0; iter.Next(); i++ {
		// 获取键值对
		// key := iter.Key()
		value := iter.Value()
		// 返回数据
		data := new(T)
		err := utils.BytesToStruct(value, data)
		if err != nil {
			global.Log.Error("数据转换错误")
			continue
		}

		// 判断是否需要分页
		if i >= (page-1)*pageSize && i < page*pageSize {
			results = append(results, data)
		}
	}

	// 判断是否需要继续遍历
	if len(results) < pageSize {
		return results, nil
	}

	return results[:pageSize], nil
}

// 带有过滤条件的查询
func (idx *Index[T]) FindByFilter(filter func(key string, value *T) bool, page, pageSize int) ([]*T, error) {
	// 构建查询范围
	// start := []byte{}
	// limit := []byte{}

	// 获取一个带有前缀的迭代器 带有布隆过滤器
	iter := idx.db.NewIterator(nil, nil)
	defer iter.Release()

	// 初始化结果集
	results := make([]*T, 0)

	// 遍历数据
	for iter.Next() {
		// 获取键值对
		key := iter.Key()
		value := iter.Value()
		// 返回数据
		data := new(T)
		err := utils.BytesToStruct(value, data)
		if err != nil {
			global.Log.Error("数据转换错误")
			continue
		}

		// 判断是否需要分页
		if filter(string(key), data) {
			results = append(results, data)
		}

		// 判断是否需要继续遍历
		// if len(results) >= pageSize {
		// 	break
		// }
	}

	// 判断是否需要继续遍历
	// if len(results) < pageSize {
	// 	return results, nil
	// }

	resData := make([]*T, len(results))
	// 将数据倒序
	for i := len(results) - 1; i >= 0; i-- {
		resData[len(results)-1-i] = results[i]
	}

	return resData, nil
	// return results, nil
}
