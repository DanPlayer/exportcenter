package redis

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"sync"
	"time"
)

var ctx = context.Background()

var Client = New(ctx, Options{
	Addr:     "127.0.0.0:6379",
	Password: "",
	DB:       0,
})

const GlobalEvent = "global_event"

type Redis struct {
	// 订阅服务器实例
	Point *redis.Client
	// 订阅列表
	PbFns sync.Map
	// 读写锁
	lock sync.Mutex
}

type Options struct {
	Addr     string
	Password string
	DB       int
}

func New(ctx context.Context, options Options) *Redis {
	instance := Redis{}
	// 实例化连接池，解决每次重新连接效率低的问题
	instance.Point = redis.NewClient(&redis.Options{
		Addr:     options.Addr,
		Password: options.Password,
		DB:       options.DB,
	})

	instance.PbFns = sync.Map{}
	go func() {
		pubSub := instance.Point.Subscribe(ctx, "__keyevent@0__:expired")
		for {
			msg, err := pubSub.ReceiveMessage(ctx)
			if err != nil {
				return
			}
			if msg.Channel == "__keyevent@0__:expired" { //nolint:nestif
				pbFnList, _ := instance.PbFns.Load(msg.Payload)
				if pbFnList != nil {
					cbList, ok := pbFnList.([]func(message string))
					if ok {
						for _, cb := range cbList {
							cb(msg.Payload)
						}
					}
				}
				// 处理全局订阅回调
				globalFnList, _ := instance.PbFns.Load(GlobalEvent)
				if globalFnList != nil {
					cbList, ok := globalFnList.([]func(message string))
					if ok {
						for _, cb := range cbList {
							cb(msg.Payload)
						}
					}
				}
			}
		}
	}()

	return &instance
}

func (r *Redis) Set(ctx context.Context, k, v string, expires time.Duration) error {
	return r.Point.Set(ctx, k, v, expires).Err()
}

func (r *Redis) Get(ctx context.Context, k string) (string, error) {
	data, err := r.Point.Get(ctx, k).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return data, nil
}

func (r *Redis) Destroy(ctx context.Context, key string) error {
	return r.Point.Del(ctx, key).Err()
}

func (r *Redis) Do(ctx context.Context, key string, time int) error {
	return r.Point.Do(ctx, key, time).Err()
}

func (r *Redis) Expire(ctx context.Context, k string, expire time.Duration) error {
	return r.Point.Expire(ctx, k, expire).Err()
}

func (r *Redis) Scan(ctx context.Context, cursor uint64, match string, count int64) (keys []string, newCursor uint64, err error) {
	return r.Point.Scan(ctx, cursor, match, count).Result()
}

func (r *Redis) HGet(ctx context.Context, k, field string) (string, error) {
	data, err := r.Point.HGet(ctx, k, field).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return data, nil
}

func (r *Redis) HSet(ctx context.Context, k string, fields ...string) error {
	return r.Point.HSet(ctx, k, fields).Err()
}

func (r *Redis) HGetAllByScan(ctx context.Context, k string) (result map[string]string, err error) {
	cursor := uint64(0)
	result = make(map[string]string)

	for {
		var keys []string
		count := int64(1000)
		keys, cursor, err = r.Point.HScan(ctx, k, cursor, "", count).Result()
		if err != nil {
			return
		}
		if len(keys) == 0 {
			break
		}
		for _, field := range keys {
			hGet, hGetErr := r.HGet(ctx, k, field)
			if hGetErr != nil {
				return
			}
			result[field] = hGet
		}
		if cursor == 0 {
			break
		}
	}

	return
}

func (r *Redis) HDel(ctx context.Context, k string, field string) error {
	return r.Point.HDel(ctx, k, field).Err()
}

func (r *Redis) CreateQueue(ctx context.Context, k string) error {
	err := r.Point.LPush(ctx, k, "").Err()
	if err != nil {
		return err
	}
	_, err = r.Point.LPop(ctx, k).Result()
	return err
}

func (r *Redis) Push(ctx context.Context, k, field string) error {
	return r.Point.LPush(ctx, k, field).Err()
}

func (r *Redis) Pop(ctx context.Context, k string) <-chan string {
	list := make(chan string)
	result, err := r.Point.LPop(ctx, k).Result()
	if err != nil {
		return list
	}

	go func() {
		for {
			if result != "" {
				list <- result
				break
			}
		}
	}()
	return list
}

// GetOriginPoint 获取原始redis实例
func (r *Redis) GetOriginPoint() *redis.Client {
	return r.Point
}

// Subscribe 订阅指定键过期时间，需要redis开启键空间消息通知：config set notify-keyspace-events Ex
func (r *Redis) Subscribe(k string, pb func(message string)) {
	var cbList []func(message string)
	pbFnList, ok := r.PbFns.Load(k)
	if ok {
		cbList, ok = pbFnList.([]func(message string))
		if ok {
			r.lock.Lock()
			cbList = append(cbList, pb)
			r.lock.Unlock()
		}
	} else {
		cbList = []func(message string){pb}
	}

	r.PbFns.Store(k, cbList)
}

// SubscribeAllEvents 订阅所有键过期事件
func (r *Redis) SubscribeAllEvents(pb func(message string)) {
	var cbList []func(message string)
	pbFnList, ok := r.PbFns.Load(GlobalEvent)
	if ok {
		cbList, ok = pbFnList.([]func(message string))
		if ok {
			r.lock.Lock()
			cbList = append(cbList, pb)
			r.lock.Unlock()
		}
	} else {
		cbList = []func(message string){pb}
	}

	r.PbFns.Store(GlobalEvent, cbList)
}

func (r *Redis) SetNx(ctx context.Context, key, val string, expire time.Duration) (bool, error) {
	boolCmd := r.Point.SetNX(ctx, key, val, expire)
	return boolCmd.Result()
}

func (r *Redis) Incr(ctx context.Context, key string) (int64, error) {
	intCmd := r.Point.Incr(ctx, key)
	return intCmd.Result()
}
