package db

import (
	"fmt"
	"net/url"
	"time"

	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func InitMysql(cfg *Config, log *zap.Logger) (*gorm.DB, error) {
	// pwd := utils.AesDecrypt(cfg.Password, LINE_DB_PWD_KEY)
	// pwd = url.QueryEscape(pwd)
	pwd := url.QueryEscape(cfg.Password)
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local",
		cfg.Username,
		pwd,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	)

	newLogger := logger.New(
		Writer{
			log:    log,
			BPrint: cfg.LogMode,
		},
		logger.Config{
			SlowThreshold:             200 * time.Millisecond, // Slow SQL threshold
			LogLevel:                  logger.Info,            // Log level
			IgnoreRecordNotFoundError: true,                   // Ignore ErrRecordNotFound error for logger
			Colorful:                  true,                   // Disable color
		},
	)

	var l logger.Interface

	if cfg.LogMode {
		l = newLogger.LogMode(logger.Info)
	} else {
		l = newLogger.LogMode(logger.Silent)
	}

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		// 禁用外键(指定外键时不会在mysql创建真实的外键约束)
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   l,
	})

	if err != nil {
		log.Sugar().Named("DB").Panicf("初始化mysql数据库异常: %v", err)
		// panic(fmt.Errorf("初始化mysql数据库异常: %v", err))
		return nil, err
	}

	// db.DB().SetMaxIdleConns(cfg.Mysql.MaxIdleConns)
	// db.DB().SetMaxOpenConns(cfg.Mysql.MaxOpenConns)

	return db, nil
}
