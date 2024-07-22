package utils

// LocalTime 自定义时间类型，兼容 PostgreSQL
// type LocalTime time.Time

// // MarshalJSON LocalTime 实现 json 序列化接口
// func (l LocalTime) MarshalJSON() ([]byte, error) {
// 	stamp := fmt.Sprintf("\"%s\"", time.Time(l).Format(FormatDateTimeMs))
// 	return []byte(stamp), nil
// }

// // UnmarshalJSON LocalTime 实现 json 反序列化接口
// func (l *LocalTime) UnmarshalJSON(data []byte) error {
// 	timeStr := StringUnquote(strings.TrimSpace(string(data)))
// 	if timeStr = strings.TrimSpace(timeStr); timeStr == "" {
// 		return errors.New("时间字符串不能为空！")
// 	}

// 	jsonTime, err := time.Parse(GetTimeFormat(timeStr), timeStr)
// 	*l = LocalTime(jsonTime)
// 	return err
// }

// // Value LocalTime 写入数据库之前，转换成 time.Time
// func (l LocalTime) Value() (driver.Value, error) {
// 	return time.Time(l).Format(FormatDateTimeMs), nil
// }

// // Scan 从数据库中读取数据，转换成 LocalTime
// func (l *LocalTime) Scan(v interface{}) error {
// 	var sqlTime time.Time
// 	switch vt := v.(type) {
// 	case string:
// 		// 字符串转成 time.Time 类型
// 		sqlTime, _ = time.Parse(FormatDateTimeMs, vt)
// 	case time.Time:
// 		sqlTime = vt
// 	case *time.Time:
// 		sqlTime = *vt
// 	default:
// 		return errors.New("读取 LocalTime 类型处理错误！")
// 	}
// 	*l = LocalTime(sqlTime)
// 	return nil
// }

// // Sub 实现 time.Time.Sub 方法计算 LocalTime 时间差
// func (l LocalTime) Sub(t LocalTime) time.Duration {
// 	return time.Time(l).Sub(time.Time(t))
// }

// // LocalDate 自定义日期类型，兼容 PostgreSQL
// type LocalDate time.Time

// // MarshalJSON LocalDate 实现 json 序列化接口
// func (l LocalDate) MarshalJSON() ([]byte, error) {
// 	stamp := fmt.Sprintf("\"%s\"", time.Time(l).Format(FormatDate))
// 	return []byte(stamp), nil
// }

// // UnmarshalJSON LocalDate 实现 json 反序列化接口
// func (l *LocalDate) UnmarshalJSON(data []byte) error {
// 	dateStr := StringUnquote(strings.TrimSpace(string(data)))
// 	if dateStr = strings.TrimSpace(dateStr); dateStr == "" {
// 		return errors.New("日期字符串不能为空！")
// 	}
// 	if len(dateStr) > len(FormatDate) && strings.Contains(dateStr, " ") {
// 		dateStr = strings.SplitN(dateStr, " ", 2)[0]
// 	}

// 	jsonTime, err := time.Parse(FormatDate, dateStr)
// 	*l = LocalDate(jsonTime)
// 	return err
// }

// // Value LocalDate 写入数据库之前，转换成 time.Time
// func (l LocalDate) Value() (driver.Value, error) {
// 	if &l == nil {
// 		return nil, nil
// 	}
// 	return time.Time(l).Format(FormatDate), nil
// }

// // Scan 从数据库中读取数据，转换成 LocalDate
// func (l *LocalDate) Scan(v interface{}) error {
// 	var sqlTime time.Time
// 	switch vt := v.(type) {
// 	case string:
// 		// 字符串转成 time.Time 类型
// 		sqlTime, _ = time.Parse(FormatDate, vt)
// 	case time.Time:
// 		sqlTime = vt
// 	case *time.Time:
// 		sqlTime = *vt
// 	default:
// 		return errors.New("读取 LocalDate 类型处理错误！")
// 	}
// 	*l = LocalDate(sqlTime)
// 	return nil
// }

// // Sub 实现 time.Time.Sub 方法计算 LocalDate 时间差
// func (l LocalDate) Sub(t LocalDate) time.Duration {
// 	return time.Time(l).Sub(time.Time(t))
// }

// // NowLocalTime 获取当前自定义格式时间
// func NowLocalTime() LocalTime {
// 	return LocalTime(time.Now())
// }

// // NowLocalTimePtr 获取当前自定义格式时间
// func NowLocalTimePtr() *LocalTime {
// 	localTime := LocalTime(time.Now())
// 	return &localTime
// }

// // NowLocalDate 获取当前自定义格式日期
// func NowLocalDate() LocalDate {
// 	return LocalDate(time.Now())
// }

// // 返回当前时间字符串
// func NowlocalDatetimeStr() string {
// 	return GetTimeFormat(time.Now().Format(FormatDateTimeMs))
// }

// // NowLocalDatePtr NowLocalDate 获取当前自定义格式日期
// func NowLocalDatePtr() *LocalDate {
// 	localDate := LocalDate(time.Now())
// 	return &localDate
// }

// // GetTimeFormat 根据日期时间字符串获取日期时间格式
// func GetTimeFormat(timeStr string) (format string) {
// 	switch len(timeStr) {
// 	case 4:
// 		format = FormatYear
// 	case 5:
// 		format = FormatTime
// 	case 8:
// 		if strings.Contains(timeStr, ":") {
// 			format = FormatTimeSec
// 		} else if strings.Contains(timeStr, "-") {
// 			format = FormatDateShort
// 		} else {
// 			format = FormatDateNum
// 		}
// 	case 9:
// 		if strings.Contains(timeStr, "-") {
// 			format = FormatDateShort
// 		}
// 	case 10:
// 		if strings.Contains(timeStr, "-") &&
// 			!strings.Contains(timeStr, ":") &&
// 			!strings.Contains(timeStr, ".") {
// 			format = FormatDate
// 		} else {
// 			format = FormatTimeMs
// 		}
// 	case 11, 12:
// 		format = FormatTimeMs
// 	case 16:
// 		// yyyy-MM-dd HH:mm
// 		format = FormatDateTime
// 	case 19:
// 		// yyyy-MM-dd HH:mm:ss
// 		format = FormatDateTimeSec
// 	case 21, 22, 23:
// 		// yyyy-MM-dd HH:mm:ss.SSS
// 		format = FormatDateTimeMs
// 	default:
// 	}
// 	return
// }

// /**
//  * @Description: 长时间
//  */
// type LongDateTime time.Time

// // UnmarshalJSON LongDateTime 实现 json 反序列化接口
// func (t *LongDateTime) UnmarshalJSON(data []byte) (err error) {
// 	now, err := time.ParseInLocation(`"`+FormatDateTimeMs+`"`, string(data), time.Local)
// 	*t = LongDateTime(now)
// 	return
// }

// // MarshalJSON LongDateTime 实现json 序列化
// func (t LongDateTime) MarshalJSON() ([]byte, error) {
// 	tTime := time.Time(t)
// 	if tTime.IsZero() {
// 		return []byte("null"), nil
// 	}
// 	return []byte(fmt.Sprintf("\"%v\"", tTime.Format(FormatDateTimeMs))), nil
// }

// func (t LongDateTime) String() string {
// 	return time.Time(t).Format(FormatDateTimeMs)
// }

// func (t LongDateTime) Value() (driver.Value, error) {
// 	var zeroTime time.Time
// 	tlt := time.Time(t)
// 	//判断给定时间是否和默认零时间的时间戳相同
// 	if tlt.UnixNano() == zeroTime.UnixNano() {
// 		return nil, nil
// 	}
// 	return tlt, nil
// }

// func (t *LongDateTime) Scan(v interface{}) error {
// 	if value, ok := v.(time.Time); ok {
// 		*t = LongDateTime(value)
// 		return nil
// 	}
// 	return fmt.Errorf("can not convert %v to timestamp", v)
// }
