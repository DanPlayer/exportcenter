package test

import (
	"exportcenter"
	"exportcenter/rabbitmq"
	"exportcenter/redis"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"os"
	"testing"
	"time"
)

var db *gorm.DB

func main(m *testing.M) {
	fmt.Println("main test")

	// 初始化数据库
	dial := mysql.Open("root:root@tcp(127.0.0.1:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local")
	var err error
	if db, err = gorm.Open(dial, &gorm.Config{
		// 设置数据库表名称为单数(User,复数Users末尾自动添加s)
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   "ec",
			SingularTable: true,
		},
	}); err != nil {
		return
	}
}

func TestRabbitMqTaskExport(t *testing.T) {
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
		"test_mq",
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
		"test_redis",
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
