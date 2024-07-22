package config

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"golang.corp.yxkj.com/orange/cadb/pkg/db"
	"golang.corp.yxkj.com/orange/cadb/pkg/logger"
	"golang.corp.yxkj.com/orange/cadb/pkg/utils"
	"gorm.io/gorm"
)

var configDB *gorm.DB

type Result struct {
	Param
}

func (r *Result) String() string {
	return r.Value
}

func (r *Result) Int64() int64 {
	i, err := strconv.ParseInt(r.Value, 10, 64)
	if err != nil {
		return 0
	}

	return i
}

func (r *Result) Int() int {
	i, err := strconv.Atoi(r.Value)
	if err != nil {
		return 0
	}

	return i
}

func (r *Result) Float64() float64 {
	i, err := strconv.ParseFloat(r.Value, 64)
	if err != nil {
		return 0
	}

	return i
}

func (r *Result) Bool() bool {
	i, err := strconv.ParseBool(r.Value)
	if err != nil {
		return false
	}

	return i
}

func (r *Result) Datetime() *time.Time {
	t, err := time.ParseInLocation("2006-01-02 15:04:05", r.Value, time.Local)
	if err != nil {
		return nil
	}

	return &t
}

func (r *Result) ToJson() string {
	name, err := json.Marshal(r)
	if err != nil {
		return ""
	}

	return string(name)
}

func (r *Result) Description() string {
	return r.Mark
}

func SetParamExist(name, value, mark string) *Result {
	r := GetParam(name, value)
	if r.ID == 0 {
		r = SetParam(name, value, mark)
	}

	return r
}

func SetParam(name, value, mark string) *Result {
	r := GetParam(name, "")
	if r.ID == 0 {
		r.ID = utils.GenID()
		r.Name = name
		r.Value = value
		r.Mark = mark

		err := getDB().Model(r).Create(r).Error
		if err != nil {
			return nil
		}
	} else {
		r.Name = name
		r.Value = value
		r.Mark = mark
		err := getDB().Model(r).Where("id = ?", r.ID).Updates(r).Error
		if err != nil {
			return nil
		}
	}

	return r
}

func GetParam(name string, DefValue string) *Result {
	r := new(Result)
	err := getDB().Model(r).Where("name = ?", name).Find(r).Error
	if err != nil {
		r.ID = 0
		r.Name = name
		r.Value = DefValue
		r.Mark = ""
	}

	if r.ID == 0 {
		r.Name = name
		r.Value = DefValue
	}

	return r
}

func GetParamList(condition string) ([]*Result, error) {
	list := make([]*Result, 0)

	query := getDB().Model(&Result{})
	if condition != "" {
		query = query.Where("(name like ? or mark like ?)", fmt.Sprintf("%%%s%%", condition), fmt.Sprintf("%%%s%%", condition))
	}

	err := query.Find(&list).Error
	return list, err
}

func getDB() *gorm.DB {
	if configDB == nil {
		initConfig()
	}

	return configDB
}

func initConfig() {
	// 检查文件是否存在
	path := filepath.Join("runtime", "data", "config.db")
	logPath := filepath.Join("runtime", "logs", "config.log")
	log, _, err := logger.NewZap(logPath, logger.LOM_DEBUG)
	if err != nil {
		panic(fmt.Errorf("创建日志对象失败，失败原因：%s", err))
	}

	d, err := db.InitSqlite(path, log)
	if err != nil {
		panic(fmt.Errorf("初始化配置数据库失败，失败原因：%s", err.Error()))
	}

	configDB = d

	// 初始化数据库表
	// 创建表
	d.AutoMigrate(
		&Param{},
	)
}
