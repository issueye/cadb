package utils

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	/* 各个格式化时间模版 */

	FormatDateTimeNum = "20060102150405"          // 日期时间格式数字串精确到秒
	FormatDateTimeMs  = "2006-01-02 15:04:05.999" // 日期时间格式精确到毫秒
	FormatDateTimeSec = "2006-01-02 15:04:05"     // 日期时间格式精确到秒
	FormatDateTime    = "2006-01-02 15:04"        // 日期时间格式精确到分
	FormatDate        = "2006-01-02"              // 日期格式：年-月-日，月日补 0
	FormatDateShort   = "2006-1-2"                // 日期格式：年-月-日
	FormatDateNum     = "20060102"                // 日期格式数字串：年月日
	FormatTimeMs      = "15:04:05.999"            // 时间格式精确到毫秒
	FormatTimeSec     = "15:04:05"                // 时间格式精确到秒
	FormatTime        = "15:04"                   // 时间格式精确到分
	FormatYear        = "2006"                    // 日期年份
	FormatMonth       = "01"
	FormatDay         = "02"
	FormatHour        = "15"
)

var (
	camelRe = regexp.MustCompile("(_)([a-zA-Z]+)")
	snakeRe = regexp.MustCompile("([a-z0-9])([A-Z])")
)

func GetNowStr() string {
	return time.Now().Format(FormatDateTimeMs)
}

func GetFormatDatetime(t time.Time) string {
	return t.Format(FormatDateTimeMs)
}

func Sha1(data string) string {
	t := sha1.New()
	_, _ = io.WriteString(t, data)
	return fmt.Sprintf("%x", t.Sum(nil))
}

// StringUnquote 去除字符串两边的双引号
func StringUnquote(value string) string {
	if strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
		value, _ = strconv.Unquote(value)
	}
	return value
}

func GetUUID() string {
	return uuid.New().String()
}

func GetCryptId() string {
	return Sha1(GetUUID())
}

// AddStr 组装字符串
func AddStr(args ...interface{}) string {
	var buffer bytes.Buffer //Buffer是一个实现了读写方法的可变大小的字节缓冲
	for _, arg := range args {
		data := arg.(string)
		buffer.WriteString(data)
	}
	return buffer.String()
}

// AddStrEx
// 组装字符串, 添加分割字符串
// func AddStrEx(split string, args ...interface{}) string {
// 	var buffer bytes.Buffer //Buffer是一个实现了读写方法的可变大小的字节缓冲
// 	for _, arg := range args {
// 		buffer.WriteString(cast.ToString(arg))
// 		buffer.WriteString(split)
// 	}
// 	return buffer.String()
// }

// GetPYM 获取拼音码
// func GetPYM(str string) string {
// 	s := ""
// 	data := pinyin.LazyConvert(str, nil)
// 	for _, v := range data {
// 		s += v[0:1]
// 	}
// 	return strings.ToUpper(s)
// }

// GetRandomString
// 随机生成一个指定位数的字符串
// func GetRandomString(l int) string {
// 	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
// 	bytes := []byte(str)
// 	var result []byte
// 	r := rand.New(rand.NewSource(time.Now().UnixNano()))
// 	for i := 0; i < l; i++ {
// 		result = append(result, bytes[r.Intn(len(bytes))])
// 	}
// 	return string(result)
// }
