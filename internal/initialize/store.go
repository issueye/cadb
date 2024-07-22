package initialize

import (
	"path/filepath"

	"golang.corp.yxkj.com/orange/cadb/internal/global"
	"golang.corp.yxkj.com/orange/cadb/internal/store"
	"golang.corp.yxkj.com/orange/cadb/internal/utils"
)

func InitStore() {
	path := filepath.Join("runtime", "db")
	// 文件夹不存在，则创建
	utils.PathExists(path)
	file := filepath.Join(path, "cadb.db")
	db, err := store.NewKVStore(file)
	if err != nil {
		panic(err)
	}

	global.Store = db
}
