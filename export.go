package exportcenter

import (
	"context"
	"errors"
	"fmt"
	"github.com/goccy/go-json"
	"github.com/panjf2000/ants/v2"
	"github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
	"gopkg.in/natefinch/lumberjack.v2"
	"gorm.io/gorm"
	"math"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

var DbClient *gorm.DB

type ExportCenter struct {
	Db            *gorm.DB
	Queue         Queue
	queuePrefix   string
	sheetMaxRows  int64
	poolMax       int
	goroutineMax  int
	isUploadCloud bool
	upload        func(filePath string) (string, error)
	logRootPath   string
	outTime       time.Duration
}

// Options 配置
type Options struct {
	Db            *gorm.DB                              // gorm实例
	QueuePrefix   string                                // 队列前缀
	Queue         Queue                                 // 队列配置（必须配置）
	SheetMaxRows  int64                                 // 数据表最大行数，用于生成队列key，可以用不同的队列同时并发写入数据，队列数量由【任务数据量】/【数据表最大行数】计算所得
	PoolMax       int                                   // 协程池最大数量
	GoroutineMax  int                                   // 协程最大数量
	IsUploadCloud bool                                  // 是否上传云端
	Upload        func(filePath string) (string, error) // 上传接口
	LogRootPath   string                                // 日志存储根目录
	OutTime       time.Duration                         // 超时时间
}

// Queue 队列
type Queue interface {
	CreateQueue(ctx context.Context, key string) error       // 创建队列
	Pop(ctx context.Context, key string) <-chan string       // 拉取数据
	Push(ctx context.Context, key string, data string) error // 推送数据
	Destroy(ctx context.Context, key string) error           // 删除队列
}

func NewClient(options Options) (*ExportCenter, error) {
	if options.SheetMaxRows == 0 {
		return nil, errors.New("SheetMaxRows数据表最大行数必须配置大于0")
	}
	if options.PoolMax <= 0 {
		options.PoolMax = 1 // 默认最大协程池数量
	}
	if options.GoroutineMax <= 0 {
		options.GoroutineMax = 1 // 默认最大协程数量
	}
	if options.OutTime == 0 {
		options.OutTime = 5 * time.Second // 默认超时时间
	}

	DbClient = options.Db

	// 自动创建任务表
	err := DbClient.Set("gorm:table_options", "ENGINE=InnoDB").AutoMigrate(&Task{})
	if err != nil {
		return nil, err
	}

	return &ExportCenter{
		Db:            options.Db,
		Queue:         options.Queue,
		poolMax:       options.PoolMax,
		sheetMaxRows:  options.SheetMaxRows,
		goroutineMax:  options.GoroutineMax,
		isUploadCloud: options.IsUploadCloud,
		upload:        options.Upload,
		logRootPath:   options.LogRootPath,
		outTime:       options.OutTime,
	}, nil
}

// CreateTask 创建导出任务
func (ec *ExportCenter) CreateTask(key, name, description, source, destination, format string, count int64, options ExportOptions) (uint, []string, error) {
	marshal, err := json.Marshal(options)
	if err != nil {
		return 0, nil, err
	}

	// 创建导出任务
	task := Task{
		Name:          name,
		Description:   description,
		Status:        TaskStatusWait.ParseInt(),
		ProgressRate:  0,
		Source:        source,
		Destination:   destination,
		ExportFormat:  format,
		ExportOptions: string(marshal),
		QueueKey:      key,
		CountNum:      count,
	}
	err = task.Create()
	if err != nil {
		return 0, nil, err
	}

	// 根据数据量，创建导出任务的数据队列
	sheetCount := int(math.Ceil(float64(count) / float64(ec.sheetMaxRows)))

	ctx := context.Background()
	keys := make([]string, 0)
	for i := 1; i <= sheetCount; i++ {
		queueKey := ""
		if ec.queuePrefix != "" {
			queueKey = fmt.Sprintf("%s_%s_sheet%d", ec.queuePrefix, task.QueueKey, i)
		} else {
			queueKey = fmt.Sprintf("%s_sheet%d", task.QueueKey, i)
		}

		err = ec.Queue.CreateQueue(ctx, queueKey)
		if err != nil {
			return 0, nil, err
		}

		keys = append(keys, queueKey)
	}

	return task.ID, keys, err
}

// PushData 推送导出数据到队列
func (ec *ExportCenter) PushData(key string, data string) error {
	ctx := context.Background()
	return ec.Queue.Push(ctx, key, data)
}

// PopData 拉取队列数据
func (ec *ExportCenter) PopData(key string) <-chan string {
	ctx := context.Background()
	return ec.Queue.Pop(ctx, key)
}

// GetTask 获取任务信息
func (ec *ExportCenter) GetTask(id int64) (info Task, err error) {
	task := Task{}
	return task.FindByID(id)
}

// CompleteTask 完成任务
func (ec *ExportCenter) CompleteTask(id, writeNum int64) error {
	task := Task{}
	return task.CompleteTaskByID(id, writeNum)
}

// ConsultTask 任务进行中
func (ec *ExportCenter) ConsultTask(id int64) error {
	task := Task{}
	return task.UpdateStatusByID(id, TaskStatusConsult)
}

// FailTask 任务失败
func (ec *ExportCenter) FailTask(id int64, errNum, writeNum int64) error {
	task := Task{}
	return task.FailTaskByID(id, errNum, writeNum)
}

// UpdateTaskDownloadUrl 更新任务文件下载链接
func (ec *ExportCenter) UpdateTaskDownloadUrl(id int64, url string) error {
	task := Task{}
	return task.UpdateDownloadUrlByID(id, url)
}

// UpdateTaskErrLogUrl 更新错误日志地址
func (ec *ExportCenter) UpdateTaskErrLogUrl(id int64, url string) error {
	task := Task{}
	return task.UpdateErrLogUrlByID(id, url)
}

// ExportToExcel 导出成excel表格，格式
func (ec *ExportCenter) ExportToExcel(id int64, filePath string, before func(key string) error) (err error) {
	// 创建日志文件
	logPath := fmt.Sprintf("/export_log/export-system(task_id-%d).log", id)
	logger := &lumberjack.Logger{
		Filename:   ec.logRootPath + logPath,
		MaxSize:    500,  // 日志文件大小，单位是 MB
		MaxBackups: 3,    // 最大过期日志保留个数
		MaxAge:     28,   // 保留过期文件最大时间，单位 天
		Compress:   true, // 是否压缩日志，默认是不压缩。这里设置为true，压缩日志
	}
	logrus.SetOutput(logger)

	logrus.WithFields(logrus.Fields{
		"task_id": id,
		"time":    time.Now(),
	}).Info("task start")

	defer func() {
		logrus.WithFields(logrus.Fields{
			"task_id": id,
			"time":    time.Now(),
		}).Info("task end")

		// 保存日志地址
		_ = ec.UpdateTaskErrLogUrl(id, logPath)
	}()

	// 获取任务信息
	task, err := ec.GetTask(id)
	if err != nil {
		logrus.Error(err)
		return err
	}

	err = ec.ConsultTask(id)
	if err != nil {
		logrus.Error(err)
		return err
	}

	// 根据数据量，创建导出任务的数据队列
	sheetCount := int(math.Ceil(float64(task.CountNum) / float64(ec.sheetMaxRows)))

	// 生成或者打开excel
	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			logrus.Error(err)
			fmt.Println(err)
		}
	}()

	// 流式写入器字典
	swMap := make(map[int32]*excelize.StreamWriter, 0)

	// 获取表格标题
	options := ExportOptions{}
	err = json.Unmarshal([]byte(task.ExportOptions), &options)
	if err != nil {
		return err
	}

	for i := 1; i <= sheetCount; i++ {
		queueKey := ""
		if ec.queuePrefix != "" {
			queueKey = fmt.Sprintf("%s_%s_sheet%d", ec.queuePrefix, task.QueueKey, i)
		} else {
			queueKey = fmt.Sprintf("%s_sheet%d", task.QueueKey, i)
		}

		err = before(queueKey)
		if err != nil {
			logrus.Error(err)
			return err
		}

		currentSheet := fmt.Sprintf("Sheet%d", i)

		if i > 1 {
			// 创建sheet
			_, err = f.NewSheet(fmt.Sprintf("Sheet%d", i))
			if err != nil {
				logrus.Error(err)
				return err
			}
		}

		// 获取写入器
		sw, err := f.NewStreamWriter(currentSheet)
		if err != nil {
			fmt.Println(err)
			return err
		}
		swMap[int32(i)] = sw

		var headers []interface{}
		for _, s := range options.Header {
			headers = append(headers, s)
		}
		// 生成标题
		cell, _ := excelize.CoordinatesToCellName(1, 1)
		err = sw.SetRow(cell, headers)
		if err != nil {
			logrus.Error(err)
			return err
		}
	}

	// 判断sheet的数据量是否达到限制，达到限制则增加数据到下一张sheet，设置当前数据增加的sheet索引值
	count := int64(0)
	rowCount := int64(1)
	errRowCount := int64(0)

	// 创建并发工作组，在工作组中使用协程处理数据写入，单个协程会有一个小时的过期时间，一个小时内未完成单表设置的最大数量就会任务失败
	var wg sync.WaitGroup
	p, _ := ants.NewPoolWithFunc(ec.poolMax, func(sheetIndex interface{}) {
		currentSheetIndex := sheetIndex.(int32)
		queueKey := ""
		if ec.queuePrefix != "" {
			queueKey = fmt.Sprintf("%s_%s_sheet%d", ec.queuePrefix, task.QueueKey, currentSheetIndex)
		} else {
			queueKey = fmt.Sprintf("%s_sheet%d", task.QueueKey, currentSheetIndex)
		}

		// 拉取队列数据
		for {
			currentRowNum := atomic.LoadInt64(&rowCount) + 1 // 当前行
			currentCount := atomic.LoadInt64(&count)

			out := false
			select {
			case data := <-ec.PopData(queueKey):
				if data == "" {
					// 记录错误数据数
					atomic.AddInt64(&errRowCount, 1)
					continue
				}

				var values interface{}
				err = json.Unmarshal([]byte(data), &values)
				if err != nil {
					// 记录错误数据数
					atomic.AddInt64(&errRowCount, 1)
					logrus.Error(err)
					continue
				}

				cell, err := excelize.CoordinatesToCellName(1, int(currentRowNum))
				if err != nil {
					// 记录错误数据数
					atomic.AddInt64(&errRowCount, 1)
					continue
				}

				slice := ec.interfaceToSlice(values)

				// 写入excel文件
				err = swMap[currentSheetIndex].SetRow(cell, slice)
				if err != nil {
					// 记录错误数据数
					atomic.AddInt64(&errRowCount, 1)
					continue
				}
			case <-time.After(ec.outTime):
				out = true
				outErr := fmt.Sprintf("%d行写入数据超时", currentRowNum)
				fmt.Println(outErr)
				logrus.Error(outErr)
				break
			}

			if out {
				break
			}

			// 增加数据到当前sheet并记录当前数据行索引，达到限制新增sheet，并重置当前sheet索引值
			atomic.AddInt64(&count, 1) // 记录数据进度
			atomic.AddInt64(&rowCount, 1)
			if currentRowNum > ec.sheetMaxRows || currentCount >= task.CountNum {
				atomic.StoreInt64(&rowCount, 0)
				_ = swMap[currentSheetIndex].Flush()
				break
			}
		}

		wg.Done()
	}, ants.WithExpiryDuration(3600))
	defer p.Release()
	// 提交协程任务
	for i := 0; i < sheetCount; i++ {
		wg.Add(1)
		_ = p.Invoke(int32(i + 1))
	}
	wg.Wait()

	// 任务进度完成（数据量达到总数包括错误数据），删除队列
	if count >= task.CountNum {
		err = ec.CompleteTask(id, count)
		if err != nil {
			logrus.Error(err)
			return err
		}
	} else {
		// 任务失败
		_ = ec.FailTask(id, errRowCount, count)
	}

	// 销毁队列
	ctx := context.Background()
	for i := 1; i <= sheetCount; i++ {
		queueKey := ""
		if ec.queuePrefix != "" {
			queueKey = fmt.Sprintf("%s_%s_sheet%d", ec.queuePrefix, task.QueueKey, i)
		} else {
			queueKey = fmt.Sprintf("%s_sheet%d", task.QueueKey, i)
		}
		_ = ec.Queue.Destroy(ctx, queueKey)
	}

	// 根据指定路径保存文件
	if err := f.SaveAs(filePath); err != nil {
		logrus.Error(err)
		return err
	}

	if ec.isUploadCloud {
		// 将文件上传至云端，记录下载地址
		url, err := ec.upload(filePath)
		if err != nil {
			logrus.Error(err)
			return err
		}

		err = ec.UpdateTaskDownloadUrl(id, url)
		if err != nil {
			logrus.Error(err)
			return err
		}

		// 删除本地文件
		err = os.Remove(filePath)
		if err != nil {
			logrus.Error(err)
			return err
		}
	} else {
		err = ec.UpdateTaskDownloadUrl(id, filePath)
		if err != nil {
			logrus.Error(err)
			return err
		}
	}

	return
}

func (ec *ExportCenter) interfaceToSlice(obj interface{}) []interface{} {
	var list []interface{}
	if reflect.TypeOf(obj).Kind() == reflect.Slice {
		s := reflect.ValueOf(obj)
		for i := 0; i < s.Len(); i++ {
			ele := s.Index(i)
			list = append(list, ele.Interface())
		}
	}
	return list
}
