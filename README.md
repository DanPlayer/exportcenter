# exportcenter
大数据导出中心，生成excel

## 简介
exportcenter能够简便的帮助大家构建大数据导出系统，exportcenter载入后提供导出任务的自动创建，以及大数据量并发安全写入excel

## 获取
```
go get github.com/DanPlayer/exportcenter
```

## 使用
#### 初始化
```
getWd, _ := os.Getwd()
// 开启导出中心
center, err := exportcenter.NewClient(exportcenter.Options{
    Db:           db,                                 // 数据库实例
    QueuePrefix:  "ec_",                              // 队列前缀
    Queue:        redis.Client,                       // 使用redis队列
    SheetMaxRows: 500000,                             // 表格最大接收行数
    PoolMax:      2,                                  // 最大并发池
    GoroutineMax: 30,                                 // 最大协程数
    LogRootPath:  fmt.Sprintf("%s/%s", getWd, "log"), // 日志文件存储地址
    OutTime:      5 * time.Second,
})
if err != nil {
    return
}
```

#### 创建导出任务
```
// 创建任务
// 返回值：
// id:任务ID
// keys:任务队列所使用的所有key值，这些key需要用户自己去使用，比如同时向所有队列推入数据，这样的话，导出数据的效率会更高
// 你也可以只向keys[0]去推送数据，后面的数据会根据SheetMaxRows自动增加到下一个队列，但是这样的效率意味着只使用了一个队列
id, keys, err := center.CreateTask(
    "test",
    "test_name",
    "test_file",
    "测试使用",
    "本地处理的数据",
    "xlsx",
    2,
    exportcenter.ExportOptions{
        Header: []string{
            "header1",
            "header2",
            "header3",
        },
    },
)
```

#### 导入数据
```
err := center.PushData(key, datum)
if err != nil {
    return
}
```

#### 导出表格
```
err = center.ExportToExcel(int64(id), "./test.xlsx", func(key string) error {
    // 在数据导入表格之前，做你想做的事情
    
    return nil
})
if err != nil {
    return
}
```

#### 日志生成
LogRootPath 配置后，会将日志自动写入该目录下，并且会根据时间7天来分割日志，保存时间为28天，同一日志最多保存3个，计划将此配置化

## 案例
#### Redis案例
```
func TestRedisTaskExport(t *testing.T) {
	getWd, _ := os.Getwd()
	// 开启导出中心
	center, err := exportcenter.NewClient(exportcenter.Options{
		Db:           db,                                 // 数据库实例
		QueuePrefix:  "ec_",                              // 队列前缀
		Queue:        redis.Client,                       // 使用redis队列
		SheetMaxRows: 500000,                             // 表格最大接收行数
		PoolMax:      2,                                  // 最大并发池
		GoroutineMax: 30,                                 // 最大协程数
		LogRootPath:  fmt.Sprintf("%s/%s", getWd, "log"), // 日志文件存储地址
		OutTime:      5 * time.Second,
	})
	if err != nil {
		return
	}

	id, keys, err := center.CreateTask(
		"test",
		"test_name",
		"test_file",
		"测试使用",
		"本地处理的数据",
		"xlsx",
		2,
		exportcenter.ExportOptions{
			Header: []string{
				"header1",
				"header2",
				"header3",
			},
		},
	)

	for _, key := range keys {
		data := []string{
			"[\"get1\",\"get1\",\"get1\"]",
		}
		for _, datum := range data {
			err := center.PushData(key, datum)
			if err != nil {
				return
			}
		}
	}

	err = center.ExportToExcel(int64(id), "./test.xlsx", nil)
	if err != nil {
		return
	}
}
```

#### RabbitMQ案例
```
func demo() {
	getWd, _ := os.Getwd()
	// 开启导出中心
	center, err := exportcenter.NewClient(exportcenter.Options{
		Db:           db,                                 // 数据库实例
		QueuePrefix:  "ec_",                              // 队列前缀
		Queue:        rabbitmq.Client,                    // 使用mq队列
		SheetMaxRows: 500000,                             // 表格最大接收行数
		PoolMax:      2,                                  // 最大并发池
		GoroutineMax: 30,                                 // 最大协程数
		LogRootPath:  fmt.Sprintf("%s/%s", getWd, "log"), // 日志文件存储地址
		OutTime:      5 * time.Second,
	})
	if err != nil {
		return
	}

	id, keys, err := center.CreateTask(
		"test",
		"test_name",
		"test_file",
		"测试使用",
		"本地处理的数据",
		"xlsx",
		2,
		exportcenter.ExportOptions{
			Header: []string{
				"header1",
				"header2",
				"header3",
			},
		},
	)

	for _, key := range keys {
		data := []string{
			"[\"get1\",\"get1\",\"get1\"]",
		}
		for _, datum := range data {
			err := center.PushData(key, datum)
			if err != nil {
				return
			}
		}
	}

	err = center.ExportToExcel(int64(id), "./test.xlsx", func(key string) error {
		// 重新开启消费者
		err := rabbitmq.Client.DeclareConsume(key)
		if err != nil {
			fmt.Println(err)
		}
		return nil
	})
	if err != nil {
		return
	}
}
```