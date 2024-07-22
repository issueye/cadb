package utils

import (
	"os"
	"path/filepath"
	"strings"
)

// 获取程序运行目录
func GetWorkDir() string {
	pwd, _ := os.Getwd()
	return pwd
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		// 创建文件夹
		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return false, err
		} else {
			return true, nil
		}
	}
	return false, err
}

func FileGetName(path string) string {
	base := filepath.Base(path)
	fileSuffix := filepath.Ext(base)
	return strings.TrimSuffix(base, fileSuffix)
}

func FileExist(path string) bool {
	_, err := os.Lstat(path)
	return !os.IsNotExist(err)
}
