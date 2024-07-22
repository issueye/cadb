package utils

import (
	"strings"
	"unsafe"
)

type Strings struct {
	Str string
}

func (s Strings) StringToByteSlice() []byte {
	tmp1 := (*[2]uintptr)(unsafe.Pointer(&s.Str))
	tmp2 := [3]uintptr{tmp1[0], tmp1[1], tmp1[1]}
	return *(*[]byte)(unsafe.Pointer(&tmp2))

}

func (s Strings) ByteSliceToString(bytes []byte) string {
	return *(*string)(unsafe.Pointer(&bytes))
}

// RepairStr
// 字符串补位
// 参数：str string 补位字符或字符串
// 参数：l int64 需要补位的长度
// 参数：t int64 类型 0 前补位 1 后补位
func (s Strings) RepairStr(str string, l, t int) string {
	data := strings.Split(s.Str, "")
	if l <= len(data) {
		return s.Str
	}

	for i := 0; i < l-len(data); i++ {
		if t == 1 {
			s.Str += str
		} else {
			s.Str = str + s.Str
		}
	}
	return s.Str
}

// StrPad
// input string 原字符串
// padLength int 规定补齐后的字符串位数
// padString string 自定义填充字符串
// padType string 填充类型:LEFT(向左填充,自动补齐位数), 默认右侧
func StrPad(input string, padLength int, padString string, padType string) string {
	output := ""
	inputLen := len(input)

	if inputLen >= padLength {
		return input
	}
	padStringLen := len(padString)
	needFillLen := padLength - inputLen

	if diffLen := padStringLen - needFillLen; diffLen > 0 {
		padString = padString[diffLen:]
	}
	for i := 1; i <= needFillLen; i += padStringLen {
		output += padString
	}
	switch padType {
	case "LEFT":
		return output + input
	default:
		return input + output
	}
}
