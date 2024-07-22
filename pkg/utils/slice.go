package utils

import "reflect"

// DeleteElement
// 字符串切片删除元素
func DeleteElement(slice []string, element string) []string {
	for i, v := range slice {
		if v == element {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

// InArray
// 判断元素是否在数组中
func InArray(arr []string, element string) bool {
	for _, v := range arr {
		if v == element {
			return true
		}
	}
	return false
}

func ListToMap(list interface{}, key string) map[string]interface{} {
	res := make(map[string]interface{})
	arr := ToSlice(list)
	for _, row := range arr {
		immutable := reflect.ValueOf(row)
		var val string
		if immutable.Kind() == reflect.Ptr {
			val = immutable.Elem().FieldByName(key).String()
		} else {
			val = immutable.FieldByName(key).String()
		}
		res[val] = row
	}
	return res
}

func ToSlice(arr interface{}) []interface{} {
	ret := make([]interface{}, 0)
	v := reflect.ValueOf(arr)
	if v.Kind() != reflect.Slice {
		ret = append(ret, arr)
		return ret
	}
	l := v.Len()
	for i := 0; i < l; i++ {
		ret = append(ret, v.Index(i).Interface())
	}
	return ret
}
