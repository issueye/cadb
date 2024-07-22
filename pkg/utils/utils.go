package utils

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
)

// StringBuilder 高性能构建字符串工具函数
func StringBuilder(values ...interface{}) string {
	builder := strings.Builder{}
	for _, value := range values {
		_, _ = fmt.Fprintf(&builder, "%v", value)
	}
	return builder.String()
}

// FillZero
// 补 0 返回字符串
//
//	value：传入值；length：总位数
func FillZero(value, length int) string {
	if length <= 0 {
		length = len(strconv.Itoa(value))
	}
	strVal := strconv.Itoa(value)
	count := length - len(strVal)
	if count > 0 {
		strVal = strings.Repeat("0", count) + strVal
	}
	return strVal
}

// MD5V
// MD5 单向加密
func MD5V(str string) string {
	h := md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
}

// NowTimeStr
// 获取指定格式的当前时间字符串
func NowTimeStr(format string) string {
	return time.Now().Format(format)
}

// NowTimePtr
// 获取当前时间指针
func NowTimePtr() *time.Time {
	now := time.Now()
	return &now
}

// IsEmptyPtr
// 判断字符串指针是否为空
func IsEmptyPtr(inStr *string) bool {
	return inStr == nil || IsNullStr(*inStr)
}

// ParseHSM
// 解析时分秒字符串
func ParseHSM(timeStr string) (time.Time, error) {
	now := time.Now()
	tmpTimeStr := ""
	if len(timeStr) <= 5 {
		tmpTimeStr = now.Format("2006-01-02") + " " + timeStr + ":00"
	}
	if len(timeStr) > 5 && len(timeStr) <= 8 {
		tmpTimeStr = now.Format("2006-01-02") + " " + timeStr
	}
	return time.ParseInLocation("2006-01-02 15:04:05", tmpTimeStr, time.Local)
}

func ParseDate(timeStr string) time.Time {
	t, err := time.Parse("2006-01-02", timeStr)
	if err != nil {
		panic(err)
	}
	return t
}

// IsNullStr
// 判断字符串是否为空 "" / "null" / "nil" / "undefined"
func IsNullStr(inStr string) bool {
	inStr = strings.TrimSpace(inStr)
	return inStr == "" || inStr == "null" || inStr == "nil" || inStr == "undefined"
}

// IsStrHasAnyPrefix 判断字符串 s 是否以 prefixes 中的任意一个前缀开头
func IsStrHasAnyPrefix(s string, prefixes []string) (prefixIndex int, has bool) {
	for i, prefix := range prefixes {
		if has = strings.HasPrefix(s, prefix); has {
			prefixIndex = i
			break
		}
	}
	return
}

// StrAllLetter
// 判断字符串是否全部是字母
func StrAllLetter(str string) bool {
	match, _ := regexp.MatchString(`^[A-Za-z]+$`, str)
	return match
}

// SubStr
// 截取指定长度的字符串
func SubStr(str string, length int) (ret string) {
	var count int
	for i := 0; i < len(str); {
		r, size := utf8.DecodeRuneInString(str[i:])
		i += size
		ret += string(r)
		count++
		if length <= count {
			break
		}
	}
	return
}

// CoveringUrl
func CoveringUrl(url string) string {
	if url[len(url)-1:] == "/" {
		return url
	} else {
		return url + "/"
	}
}

// FilterNoEmptyRepeatValues
// 过滤切片数组中非空且不重复的值
func FilterNoEmptyRepeatValues(values []string) (filterValues []string) {
	for _, value := range values {
		value = strings.TrimSpace(value)
		_, existed := SliceContainsStr(filterValues, value)
		if existed || value == "" {
			continue
		}
		filterValues = append(filterValues, value)
	}
	return
}

// SplitNoEmptyValues 根据指定符号分割字符串，并获取分割列表中的非空值
func SplitNoEmptyValues(value, sep string) (filterValues []string) {
	values := strings.Split(value, sep)
	filterValues = FilterNoEmptyRepeatValues(values)
	return
}

// SliceContainsStr 判断 string 切片中是否包含某个值
func SliceContainsStr(slice []string, value string) (index int, has bool) {
	for i, s := range slice {
		if s == value {
			index = i
			has = true
			break
		}
	}
	return
}

// SliceContainsInt 判断 int 切片中是否包含某个值
func SliceContainsInt(slice []int, value int) (index int, has bool) {
	for i, s := range slice {
		if s == value {
			index = i
			has = true
			break
		}
	}
	return
}

// SliceRemoveRepeatedStr 字符串型切片去重
func SliceRemoveRepeatedStr(strings []string) []string {
	result := make([]string, 0)
	// map 用于保存已存在的元素
	m := make(map[string]bool)
	for _, v := range strings {
		if _, ok := m[v]; !ok {
			result = append(result, v)
			m[v] = true
		}
	}
	return result
}

// SliceRemoveRepeatedInt 整型切片去重
func SliceRemoveRepeatedInt(integers []int) []int {
	result := make([]int, 0)
	// map 用于保存已存在的元素
	m := make(map[int]bool)
	for _, v := range integers {
		if _, ok := m[v]; !ok {
			result = append(result, v)
			m[v] = true
		}
	}
	return result
}

// SliceRemoveStr 移除字符串型切片中的指定元素
func SliceRemoveStr(strings []string, str string) []string {
	for i := 0; i < len(strings); i++ {
		if str == strings[i] {
			strings = append(strings[:i], strings[i+1:]...)
			i = i - 1
		}
	}
	return strings
}

// SliceRemoveInt 移除字符串型切片中的指定元素
func SliceRemoveInt(integers []int, integer int) []int {
	for i := 0; i < len(integers); i++ {
		if integer == integers[i] {
			integers = append(integers[:i], integers[i+1:]...)
			i = i - 1
		}
	}
	return integers
}

// SlicePage 切片分页
func SlicePage(page, pageSize, nums int) (sliceStart, sliceEnd int) {
	if page < 0 {
		page = 1
	}

	if pageSize < 0 {
		pageSize = 10
	}

	if pageSize > nums {
		return 0, nums
	}

	// 总页数
	pageCount := int(math.Ceil(float64(nums) / float64(pageSize)))
	if page > pageCount {
		return 0, 0
	}
	sliceStart = (page - 1) * pageSize
	sliceEnd = sliceStart + pageSize

	if sliceEnd > nums {
		sliceEnd = nums
	}
	return sliceStart, sliceEnd
}

// InitTime
// 初始化时间
func InitTime() time.Time {
	t, _ := time.Parse("2006-01-02 15:04:05.999", "1899-12-30 23:59:59.999")
	return t
}

// DayDiff
// 日期差
// func DayDiff(t1, t2 time.Time) int64 {
// 	return carbon.Parse(t1.Format(FormatDateTime)).DiffAbsInDays(carbon.Parse(t2.Format(FormatDateTime)))
// }

// GetClientIp
// 从 gin 上下文中获取客户端ip地址
func GetClientIp(c *gin.Context) string {
	ip := c.ClientIP()
	if ip == "::1" {
		ip = "127.0.0.1"
	}
	return ip
}

// GetTableName
// 获取数据库表名称
func GetTableName(name string, args ...interface{}) string {
	tableName := ""
	dbName := ""
	if len(args) > 0 {
		dbName = args[0].(string)
	}
	if dbName == "" {
		tableName = fmt.Sprintf("%s", name)
	} else {
		tableName = fmt.Sprintf("%s.DBO.%s", dbName, name)
	}
	return tableName
}
