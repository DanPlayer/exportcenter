# exportcenter

导出中心

# 简介

exportcenter能够简便的帮助大家构建大数据导出系统，exportcenter载入后提供导出任务的自动创建，以及大数据量并发安全写入

# 获取

```
go get github.com/DanPlayer/exportcenter
```

# 使用

```
func demo() {
	getWd, _ := os.Getwd()
	// 开启导出中心
	center, err := exportcenter.NewClient(exportcenter.Options{
		Db:           db.MasterClient, // 数据库实例
		QueuePrefix:  "ec_", // 队列前缀
		Queue:        rabbitmq.Client, // 开始mq队列，也可以使用redis
		SheetMaxRows: 500000, // 表格最大接收行数
		PoolMax:      2, // 最大并发池
		GoroutineMax: 30, // 最大协程数
		LogRootPath:  fmt.Sprintf("%s/%s", getWd, "log"), // 日志文件存储地址
		OutTime:      5 * time.Second,
	})
	if err != nil {
		return nil
	}

	id, keys, err := center.CreateExportTask(
		"test52000",
		"test_name",
		"test_file",
		"测试使用",
		"本地处理的数据",
		"当作测试用例",
		"xlsx",
		2,
		[]string{
			"header1",
			"header2",
			"header3",
		},
	)

	for _, key := range keys {
		data := []string{
			"[\"get1\",\"get1\",\"get1\"]",
		}
		for _, datum := range data {
            err := center.PushData(key, datum)
            if err != nil {
                return rtnerr.New(err)
            }
        }
	}
	
	err := center.ExportToExcel(int64(id), "./test.xlsx", func(key string) error {
		// 重新开启消费者
		err := rabbitmq.Client.DeclareConsume(key)
		if err != nil {
			fmt.Println(err)
		}
		return nil
	})
	if err != nil {
		return err
	}
}
```