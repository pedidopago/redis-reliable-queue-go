package rq

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func testSetupRedis() *redis.Client {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	cl := redis.NewClient(&redis.Options{
		Addr:        addr,
		Password:    os.Getenv("TEST_REDIS_PASSWORD"),
		DialTimeout: time.Second * 15,
	})

	return cl
}

func TestReliableQueue(t *testing.T) {

	cl := testSetupRedis()
	defer cl.Close()

	q := Queue{
		RedisClient:           cl,
		Name:                  "microservices_tests_redis_reliable_queue",
		MessageExpiration:     time.Minute * 5,
		ListExpirationSeconds: "3600",
	}

	assert.NoError(t, q.PushMessage(context.Background(), "test message 1"))
	assert.NoError(t, q.PushMessage(context.Background(), "test message 2 | | | "))
	assert.NoError(t, q.PushMessage(context.Background(), "test message 3"))

	rmap := sync.Map{}
	rmap.Store("test message 1", 0)
	rmap.Store("test message 2 | | | ", 0)
	rmap.Store("test message 3", 0)

	ch0 := make(chan struct{})
	wg := sync.WaitGroup{}

	fn1 := func() error {
		defer wg.Done()
		<-ch0
		return q.PopMessage(context.Background(), func(msg string) error {
			vv, ok := rmap.Load(msg)
			if !ok {
				t.Fail()
				return nil
			}
			v := vv.(int)
			v++
			rmap.Store(msg, v)
			return nil
		})
	}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			_ = fn1()
		}()
	}

	close(ch0) // will make all goroutines run at the same time
	wg.Wait()

	v1, _ := rmap.Load("test message 1")
	v2, _ := rmap.Load("test message 2 | | | ")
	v3, _ := rmap.Load("test message 3")
	v4, _ := rmap.Load("test message")

	assert.Equal(t, 1, v1.(int))
	assert.Equal(t, 1, v2.(int))
	assert.Equal(t, 1, v3.(int))
	assert.Nil(t, v4)
}

func TestAck(t *testing.T) {
	cl := testSetupRedis()
	defer cl.Close()

	ctx, cf := context.WithTimeout(context.Background(), time.Second*20)
	defer cf()

	cl.RPush(ctx, "microservices_tests_redis_reliable_queue_ackers-ack", "0|bacon", "0|salad")

	q := Queue{
		RedisClient:           cl,
		Name:                  "microservices_tests_redis_reliable_queue_ackers",
		MessageExpiration:     time.Minute * 5,
		ListExpirationSeconds: "3600",
	}

	q.RestoreExpiredMessages(ctx, 0)

	it0 := cl.LRange(ctx, "microservices_tests_redis_reliable_queue_ackers", 0, -1).Val()

	assert.Equal(t, 2, len(it0))

	cl.Del(ctx, "microservices_tests_redis_reliable_queue_ackers")
}

func TestAutoAckRecover(t *testing.T) {
	cl := testSetupRedis()
	defer cl.Close()

	ctx, cf := context.WithTimeout(context.Background(), time.Second*20)
	defer cf()

	tn0 := strconv.FormatInt(time.Now().Add(time.Minute*5).Unix(), 10)
	unremovable := tn0 + "|shouldnotremove"

	cl.RPush(ctx, "microservices_tests_redis_reliable_queue_ackers_auto-ack", "0|bacon", "0|salad", unremovable)

	defer cl.Del(ctx, "microservices_tests_redis_reliable_queue_ackers_auto-ack")
	defer cl.Del(ctx, "microservices_tests_redis_reliable_queue_ackers_auto")

	q := Queue{
		RedisClient:           cl,
		Name:                  "microservices_tests_redis_reliable_queue_ackers_auto",
		MessageExpiration:     time.Minute,
		ListExpirationSeconds: "3600",
	}

	assert.NoError(t, q.PopMessage(ctx, func(msg string) error {
		assert.Equal(t, "bacon", msg)
		return nil
	}))

	assert.NoError(t, q.PopMessage(ctx, func(msg string) error {
		assert.Equal(t, "salad", msg)
		return nil
	}))

	assert.Error(t, q.PopMessage(ctx, func(msg string) error {
		assert.Equal(t, "", msg)
		return nil
	}))

	newLen := cl.LLen(ctx, "microservices_tests_redis_reliable_queue_ackers_auto-ack").Val()
	if !assert.Equal(t, int64(1), newLen) {
		slc := cl.LRange(ctx, "microservices_tests_redis_reliable_queue_ackers_auto-ack", 0, -1).Val()
		for _, v := range slc {
			t.Log(v)
		}
	}
}
