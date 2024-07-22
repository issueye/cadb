package config

const (
	CfgServerMode       = "SERVER-MODE"
	CfgServerPort       = "SERVER-PORT"
	CfgLogMaxSize       = "LOG-MAX-SIZE"
	CfgLogMaxBackups    = "LOG-MAX-BACKUPS"
	CfgLogMaxAge        = "LOG-MAX-AGE"
	CfgLogCompress      = "LOG-COMPRESS"
	CfgLogLevel         = "LOG-LEVEL"
	CfgServerApiName    = "API-NAME"
	CfgServerApiVersion = "api-version"
)

func InitConfig() {
	SetParamExist(CfgServerPort, "10065", "端口号")
	SetParamExist(CfgServerMode, "debug", `服务运行模式， debug \ release`)
	SetParamExist(CfgServerApiName, "api", "接口名称")
	SetParamExist(CfgServerApiVersion, "v1", "接口版本")

	SetParamExist(CfgLogMaxSize, "10", "日志大小")
	SetParamExist(CfgLogMaxBackups, "10", "最大备份数")
	SetParamExist(CfgLogMaxAge, "10", "保存天数")
	SetParamExist(CfgLogCompress, "true", "是否压缩")
	SetParamExist(CfgLogLevel, "-1", "日志输出等级")
}
