package db

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
)

type Config struct {
	Username string `json:"user"`     // 用户名称
	Password string `json:"password"` // 密码
	Host     string `json:"host"`     // 服务器地址
	Database string `json:"name"`     // 数据库
	Port     int    `json:"port"`     // 端口号
	Path     string `json:"path"`     // 路径
	LogMode  bool   `json:"logMode"`  // 日志模式
}

// Writer 封装的SQL打印
type Writer struct {
	log    *zap.Logger
	BPrint bool
}

func (w Writer) Printf(format string, args ...interface{}) {

	if !w.BPrint {
		return
	}

	switch len(args) {
	case 3:
	case 4:
		{
			funcPath := args[0].(string)
			data := strings.Split(funcPath, "/")
			w.log.Debug("[sql]",
				zap.Any("sql", args[3]),
				zap.Any("time", args[1]),
				zap.Any("rows", args[2]),
				zap.Any("path", data[len(data)-1]),
			)
		}
	case 5: // 错误SQL语句
		{
			funcPath := args[0].(string)
			data := strings.Split(funcPath, "/")
			// 判断如果是 SLOW SQL 则使用 warn 级别
			fmt.Println("args[2]", args[2])

			isSlow := false
			switch t := args[2].(type) {
			case int64:
				isSlow = t > 200
			case int:
				isSlow = t > 200
			case float32:
				isSlow = t > 200
			case float64:
				isSlow = t > 200
			default:
				isSlow = false
			}

			if isSlow {
				w.log.Warn("[sql]",
					zap.Any("sql", args[4]),
					zap.Any("time", args[2]),
					zap.Any("rows", args[3]),
					zap.Any("path", data[len(data)-1]),
				)
			} else {
				w.log.Error("[sql]",
					zap.Any("sql", args[4]),
					zap.Any("message", args[1]),
					zap.Any("path", data[len(data)-1]),
				)
			}
		}
	}
}
